import Mock from 'mockjs';
import faker from 'faker';
import paginator from 'cello-paginator';

const users = Mock.mock({
  'data|11': [
    {
      id() {
        return Mock.Random.guid();
      },
      username: '@name',
      role() {
        return Mock.Random.pick(['administrator', 'user']);
      },
      'organization|1': [
        {
          id() {
            return Mock.Random.guid();
          },
          name() {
            return faker.company.companyName();
          },
        },
      ],
    },
  ],
});

function tokenVerify(req, res) {
  const { token } = req.body;
  switch (token) {
    case 'admin-token':
      return res.json({
        token,
        user: {
          id: 'administrator',
          username: 'demo-operator',
          role: 'operator',
          email: 'demo.operator@public.example',
          organization: null,
        },
      });
    case 'user-token':
      return res.json({
        token,
        user: {
          id: 'user',
          username: 'user',
          role: 'user',
          email: 'user@cello.com',
          organization: null,
        },
      });
    case 'orgAdmin-token':
      return res.json({
        token,
        user: {
          id: 'org-administrator',
          username: 'orgAdmin',
          role: 'administrator',
          email: 'administrator@cello.com',
          organization: null,
        },
      });
    default:
      return res.json({});
  }
}
export default {
  'POST /api/v1/token-verify': tokenVerify,
  '/api/v1/users': (req, res) => {
    const { page = 1, per_page: perPage = 10 } = req.query;
    const result = paginator(users.data, parseInt(page, 10), parseInt(perPage, 10));
    res.send({
      total: result.total,
      data: result.data,
    });
  },
  'POST /api/v1/login': (req, res) => {
    const { password, email, type } = req.body;
    if (password === 'demo-operator-pass' && email === 'demo.operator@public.example') {
      res.send({
        token: 'admin-token',
        user: {
          id: 'administrator',
          role: 'admin',
          email: 'demo.operator@public.example',
          organization: {
            id: 'a760606b-f55d-40bb-8e06-4c9da5ad49a6',
            name: 'org1.cello.com',
          },
        },
      });
      return;
    }
    if (password === 'password' && email === 'member@cello.com') {
      res.send({
        token: 'user-token',
        user: {
          id: 'user',
          role: 'member',
          email: 'member@cello.com',
          organization: {
            id: 'a760606b-f55d-40bb-8e06-4c9da5ad49a6',
            name: 'org1.cello.com',
          },
        },
      });
      return;
    }
    res.send({
      status: 'error',
      type,
      currentAuthority: 'guest',
    });
  },
  'POST /api/v1/register': (req, res) => {
    const { email, username } = req.body;
    if (!email || email === '') {
      res.send({
        success: false,
        message: 'email is necessary!',
      });
      return;
    }
    if (!username || username === '') {
      res.send({
        success: false,
        message: 'username is necessary!',
      });
      return;
    }
    const duplicateUser = users.data.find(value => value.username === username);
    if (duplicateUser) {
      res.send({
        success: false,
        message: 'The operator already exists!',
      });
      return;
    }

    users.data.push({
      id: Mock.Random.guid(),
      username,
      email,
      role: 'operator',
      organization: null,
    });
    res.send({
      success: true,
      message: 'register success!',
    });
  },
  'GET /api/v1/500': (req, res) => {
    res.status(500).send({
      timestamp: 1513932555104,
      status: 500,
      error: 'error',
      message: 'error',
      path: '/base/category/list',
    });
  },
  'GET /api/v1/404': (req, res) => {
    res.status(404).send({
      timestamp: 1513932643431,
      status: 404,
      error: 'Not Found',
      message: 'No message available',
      path: '/base/category/list/2121212',
    });
  },
  'GET /api/v1/403': (req, res) => {
    res.status(403).send({
      timestamp: 1513932555104,
      status: 403,
      error: 'Unauthorized',
      message: 'Unauthorized',
      path: '/base/category/list',
    });
  },
  'GET /api/v1/401': (req, res) => {
    res.status(401).send({
      timestamp: 1513932555104,
      status: 401,
      error: 'Unauthorized',
      message: 'Unauthorized',
      path: '/base/category/list',
    });
  },
};
