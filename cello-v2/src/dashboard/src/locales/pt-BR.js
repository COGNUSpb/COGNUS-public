import messages from './en-US';
import login from './pt-BR/login';
import menu from './pt-BR/menu';

export default {
  ...messages,
  'navBar.lang': 'Idiomas',
  'layout.user.link.help': 'Ajuda',
  'layout.user.link.privacy': 'Privacidade',
  'layout.user.link.terms': 'Termos',
  'app.home.introduce': 'introdução',
  'app.forms.basic.title': 'Formulário básico',
  'app.forms.basic.description':
    'Páginas de formulário são usadas para coletar ou validar informações dos usuários, e formulários básicos são comuns quando há poucos dados.',
  'error.request.200': 'Servidor retornou os dados solicitados com sucesso.',
  'error.request.201': 'Dados criados ou modificados com sucesso.',
  'error.request.202': 'A solicitação entrou na fila de processamento.',
  'error.request.204': 'Dados excluídos com sucesso.',
  'error.request.400': 'Requisição inválida. O servidor não criou nem alterou dados.',
  'error.request.401': 'Usuário sem permissão (erro de token, usuário ou senha).',
  'error.request.403': 'Usuário autenticado, mas acesso proibido.',
  'error.request.404': 'Registro solicitado não existe.',
  'error.request.406': 'Formato solicitado não disponível.',
  'error.request.410': 'Recurso solicitado foi removido permanentemente.',
  'error.request.422': 'Erro de validação ao criar o objeto.',
  'error.request.500': 'Erro do servidor. Verifique o serviço.',
  'error.request.502': 'Erro de gateway.',
  'error.request.503': 'Serviço indisponível. Servidor sobrecarregado ou em manutenção.',
  'error.request.504': 'Tempo de resposta do gateway esgotado.',
  'error.network': 'Erro de rede. Verifique sua conexão.',
  'error.login.invalidCredentials': 'Usuário ou senha inválidos.',
  'error.login.expired': 'Sessão expirada ou não autenticada. Faça login novamente.',
  'error.register.duplicate': 'E-mail ou nome de operador já existe.',
  'error.request.generic': 'Erro de requisição: {status}',
  ...login,
  ...menu,
};
