/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.island.ohara.kafka;

import com.google.common.collect.ImmutableMap;
import com.island.ohara.common.exception.ExceptionHandler;
import com.island.ohara.common.exception.OharaException;
import com.island.ohara.common.exception.OharaExecutionException;
import com.island.ohara.common.exception.OharaInterruptedException;
import com.island.ohara.common.exception.OharaTimeoutException;
import com.island.ohara.common.util.Releasable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

/**
 * a helper methods used by configurator. It provide many helper method to operate kafka cluster.
 * TODO: Do we really need a jave version of "broker" APIs? I don't think it is ok to allow user to
 * touch topic directly... by chia
 *
 * <p>Every method with remote call need to overload in Default Timeout
 */
public interface BrokerClient extends Releasable {

  /**
   * start a progress to create topic
   *
   * @return creator
   */
  TopicCreator topicCreator();

  /**
   * @param topicName topic name
   * @return true if topic exists. Otherwise, false
   */
  boolean exist(String topicName);

  /**
   * describe the topic existing in kafka. If the topic doesn't exist, exception will be thrown
   *
   * @param topicName topic name
   * @return TopicDescription
   */
  default TopicDescription topicDescription(String topicName) {
    return topicDescriptions(Collections.singletonList(topicName)).get(0);
  }

  default List<TopicDescription> topicDescriptions(List<String> topicNames) {
    // Open pull request for test
    return topicDescriptions().stream()
        .filter(t -> topicNames.stream().anyMatch(n -> n.equals(t.name())))
        .collect(Collectors.toList());
  }

  /**
   * list all topics details from kafka
   *
   * @return topic details
   */
  List<TopicDescription> topicDescriptions();

  /**
   * create new partitions for specified topic
   *
   * @param topicName topic name
   * @param numberOfPartitions number of new partitions
   */
  void createPartitions(String topicName, int numberOfPartitions);

  /**
   * remove topic.
   *
   * @param topicName topic name
   */
  void deleteTopic(String topicName);

  /** @return Connection information. form: host:port,host:port */
  String connectionProps();

  /**
   * list all active brokers' ports. This is different to {@link #connectionProps()} since this
   * method will fetch the "really" active brokers from cluster. {@link #connectionProps()} may
   * include dead or nonexistent broker nodes.
   *
   * @return active brokers' ports
   */
  Map<String, Integer> brokerPorts();

  static BrokerClient of(String connectionProps) {
    Duration timeout = Duration.ofSeconds(30);
    return new BrokerClient() {

      private final AdminClient admin = AdminClient.create(toAdminProps(connectionProps));

      private final ExceptionHandler handler =
          ExceptionHandler.builder()
              .with(ExecutionException.class, (e) -> new OharaExecutionException(e.getCause()))
              .with(InterruptedException.class, OharaInterruptedException::new)
              .with(TimeoutException.class, OharaTimeoutException::new)
              .build();

      @Override
      public TopicCreator topicCreator() {
        return new TopicCreator() {

          @Override
          public Void create() {
            return handler.handle(
                () ->
                    admin
                        .createTopics(
                            Collections.singletonList(
                                new NewTopic(name, numberOfPartitions, numberOfReplications)
                                    .configs(options)))
                        .values()
                        .get(name)
                        .get(timeout.toMillis(), TimeUnit.MILLISECONDS));
          }
        };
      }

      @Override
      public boolean exist(String topicName) {
        return handler.handle(
            () ->
                admin
                    .listTopics()
                    .names()
                    .thenApply(
                        new KafkaFuture.Function<Set<String>, Boolean>() {
                          @Override
                          public Boolean apply(Set<String> strings) {
                            return strings.contains(topicName);
                          }
                        })
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS));
      }

      @Override
      public List<TopicDescription> topicDescriptions() {
        return topicDescriptions(topicNames());
      }

      @Override
      public List<TopicDescription> topicDescriptions(List<String> names) {
        return handler.handle(
            () -> {
              try {
                return admin.describeTopics(names).all()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS).entrySet().stream()
                    .map(
                        entry -> {
                          ConfigResource configKey =
                              new ConfigResource(ConfigResource.Type.TOPIC, entry.getKey());
                          try {
                            List<TopicOption> options =
                                admin.describeConfigs(Collections.singletonList(configKey)).values()
                                    .get(configKey).get().entries().stream()
                                    .map(
                                        o ->
                                            new TopicOption(
                                                o.name(),
                                                o.value(),
                                                o.isDefault(),
                                                o.isSensitive(),
                                                o.isReadOnly()))
                                    .collect(Collectors.toList());
                            return new TopicDescription(
                                entry.getValue().name(),
                                entry.getValue().partitions().size(),
                                (short) entry.getValue().partitions().get(0).replicas().size(),
                                options,
                                entry.getValue().isInternal());
                          } catch (InterruptedException e) {
                            throw new OharaInterruptedException(e);
                          } catch (ExecutionException e) {
                            throw new OharaException(e.getCause());
                          }
                        })
                    .collect(Collectors.toList());
              } catch (ExecutionException e) {
                if (e.getCause() != null) throw e.getCause();
                else throw e;
              }
            });
      }

      @Override
      public void createPartitions(String topicName, int numberOfPartitions) {
        TopicDescription current = topicDescription(topicName);
        if (current.numberOfPartitions() > numberOfPartitions)
          throw new IllegalArgumentException(
              "Reducing the number from partitions is disallowed. current:"
                  + current.numberOfPartitions()
                  + ", expected:"
                  + numberOfPartitions);
        if (current.numberOfPartitions() < numberOfPartitions) {
          handler.handle(
              () ->
                  admin
                      .createPartitions(
                          ImmutableMap.of(topicName, NewPartitions.increaseTo(numberOfPartitions)))
                      .all()
                      .get(timeout.toMillis(), TimeUnit.MILLISECONDS));
        }
      }

      @Override
      public void deleteTopic(String topicName) {
        handler.handle(
            () ->
                admin
                    .deleteTopics(Collections.singletonList(topicName))
                    .all()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS));
      }

      List<String> topicNames() {
        return handler.handle(
            () ->
                new ArrayList<>(
                    admin.listTopics().names().get(timeout.toMillis(), TimeUnit.MILLISECONDS)));
      }

      @Override
      public String connectionProps() {
        return connectionProps;
      }

      @Override
      public Map<String, Integer> brokerPorts() {
        return handler.handle(
            () ->
                admin.describeCluster().nodes().get(timeout.toMillis(), TimeUnit.MILLISECONDS)
                    .stream()
                    .collect(Collectors.toMap(Node::host, Node::port)));
      }

      /**
       * this impl will host a kafka.AdminClient so you must call the #close() to release the
       * kafka.AdminClient.
       *
       * @param brokers the kafka brokers information
       * @return a impl from BrokerClient
       */
      private Properties toAdminProps(String brokers) {
        Properties adminProps = new Properties();
        adminProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return adminProps;
      }

      @Override
      public void close() {
        admin.close();
      }
    };
  }
}
