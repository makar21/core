from logging import getLogger

logger = getLogger()


def issue_job(task_declaration):
    logger.info('Issue job {}'.format(task_declaration))

