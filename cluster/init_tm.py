import json
import os
import subprocess


def get_persistent_peers_str(nodes):
    persistent_peers = []

    for node_num, node_dict in nodes.items():
        persistent_peers.append(
            '{}@tm{}:46656'.format(node_dict['node_id'], node_num)
        )

    return ',\\\n'.join(persistent_peers)


def update_config(config_file_path, persistent_peers_str):
    with open(config_file_path, 'r') as f:
        config = f.read()

    config = config.replace(
        'create_empty_blocks = true',
        'create_empty_blocks = false'
    )

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

    persistent_peers_str = get_persistent_peers_str(nodes)

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

        update_config(config_file_path, persistent_peers_str)

if __name__ == '__main__':
    init_tm()
