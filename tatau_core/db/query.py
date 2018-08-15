"""Query implementation for MongoDB"""


def get_transaction(db, transaction_id):
    return db.transactions.find_one({'id': transaction_id})


def get_transactions(db, transaction_ids):
    try:
        return db.transactions.find({'id': {'$in': transaction_ids}}, projection={'_id': False})
    except IndexError:
        pass


def get_metadata(db, transaction_ids):
    return db.metadata.find({'id': {'$in': transaction_ids}}, projection={'_id': False})


def get_asset(db, asset_id):
    try:
        return db.assets.find_one({'id': asset_id}, {'_id': 0, 'id': 0})
    except IndexError:
        pass


def get_txids_filtered(db, asset_id, operation=None):
    match_create = {
        'operation': 'CREATE',
        'id': asset_id
    }
    match_transfer = {
        'operation': 'TRANSFER',
        'asset.id': asset_id
    }

    if operation == 'CREATE':
        match = match_create
    elif operation == 'TRANSFER':
        match = match_transfer
    else:
        match = {'$or': [match_create, match_transfer]}

    pipeline = [
        {'$match': match}
    ]
    cursor = db.transactions.aggregate(pipeline)
    return (elem['id'] for elem in cursor)


