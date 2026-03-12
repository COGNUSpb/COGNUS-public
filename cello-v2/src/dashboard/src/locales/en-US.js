/*
 SPDX-License-Identifier: Apache-2.0
*/
import exception from './en-US/exception';
import globalHeader from './en-US/globalHeader';
import login from './en-US/login';
import menu from './en-US/menu';
import pwa from './en-US/pwa';
import component from './en-US/component';
import Organization from './en-US/Organization';
import User from './en-US/operatorUser';
import form from './en-US/form';
import Agent from './en-US/Agent';
import Node from './en-US/Node';
import fabricCa from './en-US/fabric/ca';
import Network from './en-US/Network';
import Channel from './en-US/Channel';
import ChainCode from './en-US/Chaincode';

export default {
  'navBar.lang': 'Languages',
  'layout.user.link.help': 'Help',
  'layout.user.link.privacy': 'Privacy',
  'layout.user.link.terms': 'Terms',
  'app.home.introduce': 'introduction',
  'app.forms.basic.title': 'Basic Form',
  'app.forms.basic.description':
    'Form pages are used to collect or validate user information, and basic forms are common when only a few fields are required.',

  // Error messages
  'error.request.200': 'The server returned the requested data successfully.',
  'error.request.201': 'Data was created or updated successfully.',
  'error.request.202': 'The request has been accepted for processing.',
  'error.request.204': 'Data was deleted successfully.',
  'error.request.400': 'Invalid request. The server did not create or update data.',
  'error.request.401': 'Unauthorized user (token, username, or password error).',
  'error.request.403': 'Authenticated user, but access is forbidden.',
  'error.request.404': 'The requested record does not exist.',
  'error.request.406': 'The requested format is not available.',
  'error.request.410': 'The requested resource has been permanently removed.',
  'error.request.422': 'Validation error while creating the object.',
  'error.request.500': 'Server error. Check the service.',
  'error.request.502': 'Gateway error.',
  'error.request.503': 'Service unavailable. Server overloaded or under maintenance.',
  'error.request.504': 'Gateway timeout.',
  'error.network': 'Network error. Check your connection.',
  'error.login.invalidCredentials': 'Invalid username or password.',
  'error.login.expired': 'Session expired or not authenticated. Please log in again.',
  'error.register.duplicate': 'Email or operator name already exists.',
  'error.request.generic': 'Request error: {status}',

  ...exception,
  ...globalHeader,
  ...login,
  ...menu,
  ...pwa,
  ...component,
  ...Organization,
  ...Agent,
  ...User,
  ...form,
  ...Node,
  ...fabricCa,
  ...Network,
  ...Channel,
  ...ChainCode,
};
