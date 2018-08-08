from logging import getLogger

from tatau_core import settings, web3
from tatau_core.contract import NodeContractInfo

logger = getLogger()


def issue_job(task_declaration, save=True):
    logger.info('Issue job {}'.format(task_declaration))

    NodeContractInfo.unlock_account()
    if not NodeContractInfo.get_contract().does_job_exist(task_declaration.asset_id):
        job_cost = settings.TFLOPS_COST * task_declaration.estimated_tflops
        job_budget = web3.toWei(str(job_cost), 'ether')

        NodeContractInfo.get_contract().issue_job(
            task_declaration_id=task_declaration.asset_id,
            value=job_budget
        )

        if save:
            task_declaration.save()


def check_balance(task_declaration):
    logger.info('Check balance {}'.format(task_declaration))
    return True


def deposit(task_declaration):
    pass