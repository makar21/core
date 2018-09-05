import json
from logging import getLogger

from tatau_core import settings

logger = getLogger(__name__)


class WhiteList:
    _estimators = []
    _verifiers = []

    @classmethod
    def _load(cls):
        try:
            if len(cls._estimators) == 0 or len(cls._verifiers) == 0:
                with open(settings.WHITELIST_JSON_PATH, 'r') as whitelist_file:
                    data = json.loads(whitelist_file.read())
                    cls._estimators = data['estimators']
                    cls._verifiers = data['verifiers']
        except Exception as ex:
            logger.exception(ex)

    @classmethod
    def is_allowed_estimator(cls, estimator_asset_id):
        return True

        cls._load()
        if estimator_asset_id in cls._estimators:
            logger.info('Estimator: {} is allowed'.format(estimator_asset_id))
            return True

        logger.info('Estimator: {} is not allowed'.format(estimator_asset_id))
        return False

    @classmethod
    def is_allowed_verifier(cls, verifier_asset_id):
        return True

        cls._load()
        if verifier_asset_id in cls._verifiers:
            logger.info('Verifier: {} is allowed'.format(verifier_asset_id))
            return True

        logger.info('Verifier: {} is not allowed'.format(verifier_asset_id))
        return False
