import json
import os
import subprocess


def get_persistent_peers_str(nodes, this_node_num):
    persistent_peers = []

    for node_num, node_dict in nodes.items():
        if node_num == this_node_num:
            continue
        persistent_peers.append(
            '{}@tendermint-n{}:46656'.format(node_dict['node_id'], node_num)
        )

    return ',\\\n'.join(persistent_peers)


def update_config(config_file_path, nodes, node_num):
    with open(config_file_path, 'r') as f:
        config = f.read()

    config = config.replace(
        'create_empty_blocks = true',
        'create_empty_blocks = false'
    )

    config = config.replace(
        'proxy_app = "tcp://127.0.0.1:26658"',
        'proxy_app = "tcp://bigchaindb-n{}:46658"'.format(node_num)
    )

    config = config.replace('26656', '46656')

    config = config.replace('26657', '46657')

    persistent_peers_str = get_persistent_peers_str(nodes, node_num)

    config = config.replace(
        'persistent_peers = ""',
        'persistent_peers = "{},"'.format(persistent_peers_str)
    )

    with open(config_file_path, 'w') as f:
        f.write(config)


def init_tm():
    directory = os.path.dirname(os.path.abspath(__name__))
    tmdata_path = os.path.join(directory, 'tmdata')

    nodes_num = 4

    nodes = dict((x, {}) for x in range(nodes_num))

    for node_num, node_dict in nodes.items():
        node_tmdata_path = os.path.join(tmdata_path, 'n{}'.format(node_num))

        os.makedirs(node_tmdata_path, exist_ok=True)
        os.chmod(node_tmdata_path, 0o777)

        subprocess.run([
            'docker',
            'run',
            '-it',
            '--rm',
            '-v',
            '{}:/tendermint'.format(node_tmdata_path),
            'tendermint/tendermint',
            'init',
        ])

        completed_process = subprocess.run([
            'docker',
            'run',
            '-it',
            '--rm',
            '-v',
            '{}:/tendermint'.format(node_tmdata_path),
            'tendermint/tendermint',
            'show_node_id',
        ], stdout=subprocess.PIPE)

        node_dict['node_id'] = completed_process.stdout.decode().strip()

        genesis_file_path = os.path.join(
            node_tmdata_path,
            'config/genesis.json',
        )

        with open(genesis_file_path, 'r') as f:
            node_dict['genesis'] = json.loads(f.read())

    genesis_dict = nodes[0]['genesis']

    for node_num in range(1, nodes_num):
        genesis_dict['validators'].append(
            nodes[node_num]['genesis']['validators'][0]
        )

    genesis_json = json.dumps(genesis_dict, sort_keys=True, indent=2)

    for node_num in nodes:
        node_tmdata_path = os.path.join(tmdata_path, 'n{}'.format(node_num))

        genesis_file_path = os.path.join(
            node_tmdata_path,
            'config/genesis.json',
        )

        with open(genesis_file_path, 'w') as f:
            f.write(genesis_json)

        config_file_path = os.path.join(
            node_tmdata_path,
            'config/config.toml',
        )

        update_config(config_file_path, nodes, node_num)

if __name__ == '__main__':
    init_tm()
