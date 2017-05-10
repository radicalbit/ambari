/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

var App = require('app');

/**
 * @class
 *
 * This is a view for showing Kafka_BrokerTopicMetrics
 *
 * @extends App.ChartLinearTimeView
 * @extends Ember.Object
 * @extends Ember.View
 */
App.ChartServiceMetricsKafka_BrokerTopicMetrics = App.ChartLinearTimeView.extend({
  id: "service-metrics-kafka-broker-topic-metrics",
  title: Em.I18n.t('services.service.info.metrics.kafka.server.brokerTopic.title'),
  renderer: 'line',
  ajaxIndex: 'service.metrics.kafka.broker.topic',

  seriesTemplate: {
    path: 'metrics.kafka.server.BrokerTopicMetrics',
    displayName: function (name) {
      var displayNameMap = {
        AllTopicsBytesOutPerSec: Em.I18n.t('services.service.info.metrics.kafka.server.brokerTopic.displayNames.AllTopicsBytesOutPerSec'),
        AllTopicsBytesInPerSec: Em.I18n.t('services.service.info.metrics.kafka.server.brokerTopic.displayNames.AllTopicsBytesInPerSec'),
        AllTopicsMessagesInPerSec: Em.I18n.t('services.service.info.metrics.kafka.server.brokerTopic.displayNames.AllTopicsMessagesInPerSec')
      };
      return displayNameMap[name];
    }
  },

  getData: function (jsonData) {
    return this.getKafkaData(jsonData);
  }
});