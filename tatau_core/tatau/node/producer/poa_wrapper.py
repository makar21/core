from logging import getLogger

logger = getLogger()


def issue_job(task_declaration):
    logger.info('Issue job {}'.format(task_declaration))
    # TODO handle 1st deployment or redeployment because fake or failed workers

