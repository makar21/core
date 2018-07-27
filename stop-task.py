import argparse
from logging import getLogger, basicConfig, StreamHandler, INFO

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
    parser = argparse.ArgumentParser(description='Monitor Task')

    parser.add_argument('-k', '--key', default='producer', metavar='KEY', help='producer RSA key name')
    parser.add_argument('-t', '--task', metavar='TASK_ASSET', help='asset id of task declaration')

    args = parser.parse_args()

    Producer(rsa_pk_fs_name=args.key)

    task_declaration = TaskDeclaration.get(args.task)
    task_declaration.state = TaskDeclaration.State.COMPLETED
    task_declaration.save()

    logger.info('{} state: {}'.format(task_declaration, task_declaration.state))


if __name__ == '__main__':
    main()

