using RabbitMqDemoConsoleApp.Services;

// Uncomment the demo you want to run:

// await TopicExchangeDemo.Generate_Log_TopicExchange();
// await AutoDeletedQueueDemo.Generate_Log__auto_deleted_queue();
// await ExpiredQueueDemo.Generate_Log__expier_queue();
// await MessageTtlQueueDemo.Generate_Log__message_ttl_queue();
// await DeadLetterExchangeDemo.Generate_Log__message_dead_queue_letter_exchaneg();
// await DeadLetterRoutingKeyDemo.Generate_Log__message_dead_letter_routing_key();
// await MandatoryMessageDemo.Generate_Log__message_mandatory_false();
// await PrioritiesConsumerDemo.Generate_Log_message_priorities_consumer();
// await MaxLengthQueueDemo.Generate_Log_message_max_length_queue();
// await PersistenceMessageDemo.Generate_persitence_message();
// await VirtualHostUserDemo.Generate_message_wothVirtualHost_user_specific();


//await TopicPermissionDemo.Generate_Log__message_topic_permission();
await MessageTracingUseCaseService.WriteMessage();
