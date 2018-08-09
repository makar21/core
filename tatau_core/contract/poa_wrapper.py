from logging import getLogger

from tatau_core import settings, web3
from tatau_core.contract import NodeContractInfo
from tatau_core.tatau.models import TaskAssignment

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

    # if return FALSE job will not get state DEPLOYMENT (return FALSE when will work with WebUI)
    return True


def finish_job(task_declaration):
    logger.info('Finish job {}'.format(task_declaration))

    NodeContractInfo.unlock_account()
    NodeContractInfo.get_contract().finish_job(task_declaration.asset_id)


def distribute(task_declaration, verification_result):
    logger.info('Distribute job {}'.format(task_declaration))

    workers = []
    amounts = []
    total_amount = 0
    for task_assignment in task_declaration.get_task_assignments(states=TaskAssignment.State.FINISHED):
        for vr in verification_result:
            if vr['worker_id'] == task_assignment.worker.asset_id and not vr['is_fake']:
                workers.append(task_assignment.worker.account_address)
                amount = web3.toWei(str(settings.TFLOPS_COST * task_assignment.tflops), 'ether')
                total_amount += int(amount)
                amounts.append(amount)
                break

    NodeContractInfo.unlock_account()
    logger.info('Job balance: {} distribute: {}'.format(get_job_balance(task_declaration), total_amount))
    NodeContractInfo.get_contract().distribute(
        task_declaration_id=task_declaration.asset_id,
        workers=workers,
        amounts=amounts
    )

    return None


def get_job_balance(task_declaration):
    NodeContractInfo.unlock_account()
    return NodeContractInfo.get_contract().get_job_balance(task_declaration.asset_id)


def deposit(task_declaration, amount):
    logger.info('Deposit job {} on {}'.format(task_declaration, amount))

    NodeContractInfo.unlock_account()
    NodeContractInfo.get_contract().deposit(task_declaration.asset_id, web3.toWei(str(amount), 'ether'))
