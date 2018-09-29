import time
from logging import getLogger

from tatau_core.db import exceptions

logger = getLogger('tatau_core')


class Asset:
    def __init__(self, asset_id, transactions, db):
        self.db = db
        self._transactions = transactions
        self.asset_id = asset_id
        self.created_at = transactions[0].get('generation_time')
        self.modified_at = transactions[-1].get('generation_time')

    @property
    def last_tx(self):
        return self._transactions[-1]

    @property
    def first_tx(self):
        return self._transactions[0]

    @property
    def data(self):
        return self._transactions[0]['asset']['data']

    @property
    def metadata(self):
        return self._transactions[-1].get('metadata')

    @property
    def initial_metadata(self):
        return self._transactions[0].get('metadata')

    @property
    def address(self):
        return self.last_tx['outputs'][0]['public_keys'][0]

    @classmethod
    def get(cls, asset_id, db):
        transactions = db.get_transactions(asset_id)
        if len(transactions) == 0:
            raise exceptions.Asset.NotFound()

        return cls(asset_id=asset_id, transactions=transactions, db=db)

    @classmethod
    def create(cls, data, metadata, recipients, db):
        prepared_create_tx = db.bdb.transactions.prepare(
            operation='CREATE',
            signers=db.kp.public_key,
            asset={'data': data},
            recipients=recipients,
            metadata=metadata
        )

        fulfilled_create_tx = db.bdb.transactions.fulfill(
            transaction=prepared_create_tx,
            private_keys=db.kp.private_key
        )

        logger.debug('Fulfill CREATE tx {} for asset {}'.format(fulfilled_create_tx['id'], data['asset_name']))

        asset_id = fulfilled_create_tx['id']

        # check is asset already created
        logger.debug("Check is asset already created: {}".format(asset_id))
        txs = db.bdb.transactions.get(asset_id=asset_id)
        if len(txs):
            logger.debug("Asset already exists: {}".format(asset_id))
            asset = cls(asset_id=asset_id, transactions=txs, db=db)
            asset._update_if_were_changes(metadata, recipients)
            return asset, False

        from tatau_core.db.db import async_commit
        ac = async_commit()
        if ac.async:
            db.bdb.transactions.send_async(fulfilled_create_tx)
            ac.add_tx_id(fulfilled_create_tx['id'])
        else:
            db.bdb.transactions.send_commit(fulfilled_create_tx)
        return cls(asset_id=fulfilled_create_tx['id'], transactions=[fulfilled_create_tx], db=db), True

    # noinspection PyMethodMayBeStatic
    def _dicts_are_equal(self, x, y):
        shared_items = {k: x[k] for k in x if k in y and x[k] == y[k]}
        return len(shared_items) == len(x)

    def _update_if_were_changes(self, metadata, recipients):
        were_changes = True
        if recipients is not None and sorted(self.last_tx['outputs'][0]['public_keys']) != sorted(recipients):
            # owners was changed
            were_changes = False

        if self._dicts_are_equal(self.metadata, metadata):
            were_changes = False

        if were_changes:
            self.save(metadata, recipients)

    def save(self, metadata, recipients):
        previous_tx = self.last_tx

        # we cant create tx if previous tx was not committed
        while not self.db.bdb.blocks.get(txid=previous_tx['id']):
            logger.debug('Previous tx is not committed, waiting...')
            time.sleep(1)

        output_index = 0
        output = previous_tx['outputs'][output_index]

        transfer_input = {
            'fulfillment': output['condition']['details'],
            'fulfills': {
                'output_index': output_index,
                'transaction_id': previous_tx['id'],
            },
            'owners_before': output['public_keys'],
        }

        prepared_transfer_tx = self.db.bdb.transactions.prepare(
            operation='TRANSFER',
            asset={'id': self.asset_id},
            inputs=transfer_input,
            recipients=recipients or self.db.kp.public_key,
            metadata=metadata,
        )

        fulfilled_transfer_tx = self.db.bdb.transactions.fulfill(
            prepared_transfer_tx,
            private_keys=self.db.kp.private_key,
        )

        logger.debug('Fulfill TRANSFER tx {} for asset {}'.format(fulfilled_transfer_tx['id'], self.data['asset_name']))
        from tatau_core.db.db import async_commit
        ac = async_commit()
        if ac.async:
            self.db.bdb.transactions.send_async(fulfilled_transfer_tx)
            ac.add_tx_id(fulfilled_transfer_tx['id'])
        else:
            self.db.bdb.transactions.send_commit(fulfilled_transfer_tx)
        self._transactions.append(fulfilled_transfer_tx)
