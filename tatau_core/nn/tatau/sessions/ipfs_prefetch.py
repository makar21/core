import shutil
import sys
import tempfile
from logging import getLogger

from tatau_core.utils.ipfs import IPFS
from .session import Session

logger = getLogger(__name__)


class IpfsPrefetchSession(Session):
    def __init__(self, uuid=None):
        super(IpfsPrefetchSession, self).__init__(module=__name__, uuid=uuid)

    def process_assignment(self, assignment):
        pass

    def run(self, multihash):
        self._run(multihash, async=True)

    def main(self, multihash):
        logger.info('Run IPFS prefetch')
        ipfs = IPFS()
        target_dir = tempfile.mkdtemp()
        try:
            logger.info('Download {}'.format(multihash))
            ipfs.download(multihash, target_dir)
            logger.info('End download {}'.format(multihash))
        finally:
            shutil.rmtree(target_dir)


if __name__ == '__main__':
    session = IpfsPrefetchSession(uuid=sys.argv[1])
    session.main(sys.argv[2])
