from django.core.management.base import BaseCommand
from api.models import Channel, Network, Organization
from api.lib.peer.channel import Channel as PeerChannel
from api.config import CELLO_HOME

class Command(BaseCommand):
    help = 'Importa canais existentes no Fabric para o banco de dados Django.'

    def handle(self, *args, **options):
        # Descobrir todas as networks
        networks = Network.objects.all()
        for network in networks:
            print(f'Processando network: {network.name}')
            # Para cada organização da network
            orgs = Organization.objects.filter(network=network)
            for org in orgs:
                print(f'  Organização: {org.name}')
                # Inicializar PeerChannel com variáveis de ambiente da org
                try:
                    peer_channel = PeerChannel()
                    code, fabric_channels = peer_channel.list()
                    if code != 0:
                        print(f'    Erro ao listar canais: {fabric_channels}')
                        continue
                except Exception as e:
                    print(f'    Erro ao inicializar PeerChannel: {e}')
                    continue
                # Para cada canal encontrado no Fabric
                for ch_name in fabric_channels:
                    if not Channel.objects.filter(name=ch_name).exists():
                        print(f'    Importando canal órfão: {ch_name}')
                        channel = Channel(name=ch_name, network=network)
                        channel.save()
                        channel.organizations.add(org)
                        print(f'      Canal {ch_name} importado com sucesso.')
                    else:
                        print(f'    Canal {ch_name} já existe no banco.')
        print('Sincronização concluída.')
