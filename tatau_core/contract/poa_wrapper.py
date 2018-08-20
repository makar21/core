from logging import getLogger

from hexbytes import HexBytes

from tatau_core import settings, web3
from tatau_core.contract import NodeContractInfo

logger = getLogger()


def issue_job(task_declaration, job_cost):
    logger.info('Issue job {} balance {}'.format(task_declaration, job_cost))

    NodeContractInfo.unlock_account()
    if not NodeContractInfo.get_contract().does_job_exist(task_declaration.asset_id):
        job_budget = web3.toWei(str(job_cost), 'ether')

        NodeContractInfo.get_contract().issue_job(
            task_declaration_id=task_declaration.asset_id,
            value=job_budget
        )


def does_job_exist(task_declaration):
    return NodeContractInfo.get_contract().does_job_exist(task_declaration.asset_id)


def does_job_finished(task_declaration):
    return NodeContractInfo.get_contract().does_job_finished(task_declaration.asset_id)


def finish_job(task_declaration):
    logger.info('Finish job {}'.format(task_declaration))

    NodeContractInfo.unlock_account()
    NodeContractInfo.get_contract().finish_job(task_declaration.asset_id)


def distribute(verification_assignment):
    from tatau_core.tatau.models import TaskAssignment, VerificationAssignment
    task_declaration = verification_assignment.task_declaration
    verification_result = verification_assignment.result

    logger.info('Distribute job {}'.format(task_declaration))

    if verification_assignment.distribute_history is not None:
        try:
            tx_hash_str = verification_assignment.distribute_history[str(task_declaration.current_epoch)]
            tx_hash = HexBytes.fromhex(tx_hash_str)
            if NodeContractInfo.get_contract().is_transaction_mined(tx_hash):
                logger.info('Distribute for {} for epoch {} already mined'.format(
                    task_declaration, task_declaration.current_epoch))
                return
            else:
                NodeContractInfo.get_contract().wait_for_transaction_mined(tx_hash)
                return
        except KeyError:
            pass
    else:
        verification_assignment.distribute_history = {}

    workers = []
    amounts = []
    total_amount = 0
    task_assignments = task_declaration.get_task_assignments(states=(TaskAssignment.State.FINISHED,))
    for task_assignment in task_assignments:
        for vr in verification_result:
            if vr['worker_id'] == task_assignment.worker.asset_id and not vr['is_fake']:
                workers.append(web3.toChecksumAddress(task_assignment.worker.account_address))
                amount = web3.toWei(str(settings.TFLOPS_COST * task_assignment.tflops), 'ether')
                total_amount += amount
                amounts.append(amount)
                break

    verification_assignments = task_declaration.get_verification_assignments(
        states=(VerificationAssignment.State.VERIFICATION_FINISHED,)
    )

    for va in verification_assignments:
        workers.append(web3.toChecksumAddress(va.verifier.account_address))
        amount = web3.toWei(str(settings.TFLOPS_COST * va.tflops), 'ether')
        total_amount += amount
        amounts.append(amount)

    NodeContractInfo.unlock_account()
    job_balance = get_job_balance(task_declaration)
    logger.info('Job balance: {:.5f} ETH distribute: {:.5f} ETH'.format(
        web3.fromWei(job_balance, 'ether'),  web3.fromWei(total_amount, 'ether')))

    tx_hash = NodeContractInfo.get_contract().distribute_async(
        task_declaration_id=task_declaration.asset_id,
        workers=workers,
        amounts=amounts
    )

    verification_assignment.distribute_history[str(task_declaration.current_epoch)] = ''.join(
        '{:02x}'.format(x) for x in tx_hash)
    verification_assignment.save()

    NodeContractInfo.get_contract().wait_for_transaction_mined(tx_hash)


def get_job_balance(task_declaration):
    return NodeContractInfo.get_contract().get_job_balance(task_declaration.asset_id)


def deposit(task_declaration, amount):
    logger.info('Deposit job {} on {} ETH'.format(task_declaration, amount))

    NodeContractInfo.unlock_account()
    NodeContractInfo.get_contract().deposit(task_declaration.asset_id, web3.toWei(str(amount), 'ether'))
