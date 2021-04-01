import { ApolloLink, createHttpLink } from '@apollo/client';
import { getMainDefinition } from 'apollo-utilities';
import { OperationDefinitionNode } from 'graphql';

import {
  AppSyncRealTimeSubscriptionHandshakeLink
} from './realtime-subscription-handshake-link';
import { UrlInfo } from './types';

const createSubscriptionHandshakeLink = (
  infoOrUrl: UrlInfo,
  theResultsFetcherLink?: ApolloLink
): ApolloLink => {
  const { url } = infoOrUrl;
  const resultsFetcherLink: ApolloLink = theResultsFetcherLink || createHttpLink({ uri: url });
  const subscriptionLinks: ApolloLink = new AppSyncRealTimeSubscriptionHandshakeLink(infoOrUrl);

  return ApolloLink.split(
    operation => {
      const { query } = operation;
      const { kind, operation: graphqlOperation } = getMainDefinition(
        query
      ) as OperationDefinitionNode;
      const isSubscription =
        kind === 'OperationDefinition' && graphqlOperation === 'subscription';

      return isSubscription;
    },
    subscriptionLinks,
    resultsFetcherLink
  );
}

export { createSubscriptionHandshakeLink };
