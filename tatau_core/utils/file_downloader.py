
import urllib.request
from logging import getLogger
from multiprocessing.pool import ThreadPool

from tatau_core import settings

logger = getLogger()


class FileDownloader:
    class Params:
        def __init__(self, url, target_path):
            self.url = url
            self.target_path = target_path

    @staticmethod
    def _download(download_params: Params):
        logger.info('Start of downloading {} to {}'.format(download_params.url, download_params.target_path))
        for i in range(3):
            try:
                urllib.request.urlretrieve(download_params.url, download_params.target_path)
                break
            except Exception as ex:
                logger.exception(ex)
                if i == 2:
                    raise

        logger.info('Finish of downloading {} to {}'.format(download_params.url, download_params.target_path))

    @classmethod
    def download_all(cls, list_download_params, pool_size=settings.DOWNLOAD_POOL_SIZE):
        with ThreadPool(pool_size) as p:
            return p.map(cls._download, list_download_params)