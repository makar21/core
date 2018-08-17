import argparse
from logging import getLogger, basicConfig, StreamHandler, INFO

from tatau_core.contract import NodeContractInfo, poa_wrapper
from tatau_core.tatau.models import TaskDeclaration
from tatau_core.tatau.node import Producer

basicConfig(
    format='%(message)s',
    level=INFO,
    handlers=[
        StreamHandler()
    ],
)

logger = getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description='Deposit Task')

    parser.add_argument('-k', '--key', default='producer', metavar='KEY', help='producer RSA key name')
    parser.add_argument('-t', '--task', metavar='TASK_ASSET', help='asset id of task declaration')
    parser.add_argument('-b', '--balance', metavar='BALANCE_ETH', help='deposit in eth')

    args = parser.parse_args()

    NodeContractInfo.init_poa(key_name='producer')
    Producer(account_address=NodeContractInfo.get_account_address(), rsa_pk_fs_name=args.key)

    task_declaration = TaskDeclaration.get(args.task)
    poa_wrapper.issue_job(task_declaration, args.balance)


if __name__ == '__main__':
    main()

