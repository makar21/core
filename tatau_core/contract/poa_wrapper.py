from logging import getLogger

from hexbytes import HexBytes

from tatau_core import web3
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


def get_job_balance(task_declaration):
    return NodeContractInfo.get_contract().get_job_balance(task_declaration.asset_id)


def deposit(task_declaration, amount):
    logger.info('Deposit job {} on {} ETH'.format(task_declaration, amount))

    NodeContractInfo.unlock_account()
    NodeContractInfo.get_contract().deposit(task_declaration.asset_id, web3.toWei(str(amount), 'ether'))


def finish_job(task_declaration):
    logger.info('Finish job {}'.format(task_declaration))

    NodeContractInfo.unlock_account()
    NodeContractInfo.get_contract().finish_job(task_declaration.asset_id)


def distribute(task_declaration, verification_assignment):
    from tatau_core.models import TaskAssignment, WorkerPayment

    logger.info('Distribute {}'.format(task_declaration))
    iteration = task_declaration.current_iteration
    iteration_retry = task_declaration.current_iteration_retry

    good_worker_ids = []
    fake_worker_ids = []
    # if verification was failed or task was canceled
    if verification_assignment.verification_result.result:
        for r in verification_assignment.verification_result.result:
            if r['is_fake']:
                fake_worker_ids.append(r['worker_id'])
            else:
                good_worker_ids.append(r['worker_id'])

    amount_for_worker = int(task_declaration.iteration_cost_in_wei / task_declaration.workers_requested)
    distribute_history = verification_assignment.distribute_history
    distribute_transactions = distribute_history.distribute_transactions

    iteration_data = distribute_transactions.get(str(iteration))
    if not iteration_data:
        distribute_transactions[str(iteration)] = {}

    iteration_retry_data = distribute_transactions[str(iteration)].get(str(iteration_retry))
    if not iteration_retry_data:
        distribute_transactions[str(iteration)][str(iteration_retry)] = {
            'workers': [],
            'transaction': None
        }

    already_payed_workers = []
    for retry, value in distribute_transactions[str(iteration)].items():
        already_payed_workers += value['workers']

    distribute_data = distribute_transactions[str(iteration)][str(iteration_retry)]
    if distribute_data['transaction'] is not None:
        tx_hash_str = distribute_data['transaction']
        logger.info('Transaction for {} for iteration {} is {}'.format(
            task_declaration, task_declaration.current_iteration, tx_hash_str))

        tx_hash = HexBytes.fromhex(tx_hash_str)
        if NodeContractInfo.get_contract().is_transaction_mined(tx_hash):
            logger.info('Distribute for {} for iteration {} is mined'.format(
                task_declaration, task_declaration.current_iteration))
            return
        else:
            if task_declaration.last_iteration:
                NodeContractInfo.get_contract().wait_for_transaction_mined(tx_hash)
                logger.info('Distribute for {} for iteration {} is mined'.format(
                    task_declaration, task_declaration.current_iteration))
            else:
                logger.info('Distribute for {} for iteration {} is not mined'.format(
                    task_declaration, task_declaration.current_iteration))
            return

    if len(good_worker_ids) == 0:
        logger.info('No targets for distribute')
        return

    worker_addresses = []
    amounts = []
    distribute_total_amount = 0.0
    worker_payments = []

    for task_assignment in task_declaration.get_task_assignments(states=(TaskAssignment.State.FINISHED,)):
        if task_assignment.worker.asset_id in fake_worker_ids:
            continue

        if task_assignment.worker.asset_id in already_payed_workers:
            continue

        worker_addresses.append(task_assignment.worker.account_address)
        amounts.append(amount_for_worker)
        distribute_total_amount += float(web3.fromWei(amount_for_worker, 'ether'))

        worker_payments.append(
            WorkerPayment(
                db=verification_assignment.db,
                encryption=verification_assignment.encryption,
                producer_id=task_declaration.producer_id,
                worker_id=task_assignment.worker.asset_id,
                task_declaration_id=task_declaration.asset_id,
                train_iteration=task_declaration.current_iteration,
                train_iteration_retry=task_declaration.current_iteration_retry,
                tflops=task_assignment.train_result.tflops,
                tokens=float(web3.fromWei(amount_for_worker, 'ether'))
            )
        )

    NodeContractInfo.unlock_account()

    logger.info('Job balance: {:.5f} ETH distribute: {:.5f} ETH worker addresses: {}'.format(
        task_declaration.balance,  distribute_total_amount, worker_addresses))

    tx_hash = NodeContractInfo.get_contract().distribute_async(
        task_declaration_id=task_declaration.asset_id,
        workers=worker_addresses,
        amounts=amounts
    )

    distribute_data['workers'] = good_worker_ids
    distribute_data['transaction'] = ''.join('{:02x}'.format(x) for x in tx_hash)
    distribute_history.save()

    for worker_payment in worker_payments:
        worker_payment.save()

    if task_declaration.last_iteration:
        logger.info(
            'Wait for distribute of last iteration for task: {} balance: {:.5f} ETH distribute: {:.5f} ETH'.format(
                task_declaration, task_declaration.balance, distribute_total_amount))
        NodeContractInfo.get_contract().wait_for_transaction_mined(tx_hash)

    logger.info('Job {} distributed async'.format(task_declaration))
