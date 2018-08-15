package my.artifact.myeventplayer.consumer

import co.remotectrl.eventplayer.*
import co.remotectrl.kafka.util.ReflectDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import my.artifact.myeventplayer.common.aggregate.MyAggregate
import my.artifact.myeventplayer.consumer.datasource.DataSource
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.*

private val BOOTSTRAP_SERVERS = "localhost:9092"
private fun createMyAggregateConsumer(): KafkaConsumer<AggregateId<MyAggregate>, PlayEvent<MyAggregate>> {
    val props = Properties()
    props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = BOOTSTRAP_SERVERS
    props[ConsumerConfig.GROUP_ID_CONFIG] = "KafkaExampleAvroConsumer"

    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = ReflectDeserializer::class.java.getName()
    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = ReflectDeserializer::class.java.getName()

//    props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java.getName()
//    props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = KafkaAvroDeserializer::class.java.getName()

    //Use Specific Record or else you get Avro GenericRecord.
    props[KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG] = true

    //Schema registry location.
    props[KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG] = "http://localhost:8081"

//    val schemaReg = CachedSchemaRegistryClient(listOf("http://localhost:8081"), KafkaAvroDeserializerConfig.MAX_SCHEMAS_PER_SUBJECT_DEFAULT)

    return KafkaConsumer(props)
}

private val TOPIC = "MyAggregate"

fun main(args : Array<String>) {

    val consumer = createMyAggregateConsumer()
    consumer.subscribe(Collections.singletonList(TOPIC))

    try{
        consume(consumer, DataSource())
    }
    catch (e: Exception) {
        System.out.println("An unrecoverable error occurred: [${e}]")
    }
    finally {
        consumer.close()
    }
}

fun consume(consumer: KafkaConsumer<AggregateId<MyAggregate>, PlayEvent<MyAggregate>>, ds: DataSource) {
    while (true) {

        val records = consumer.poll(100)

        if(records.count() > 0){
            try {
                processEvents(records, ds)
                consumer.commitSync()
            }
            catch (e: Exception){
                System.out.println("An error occurred while trying to process a record: [${e}]")
            }
        }

    }
}

fun processEvents(records: ConsumerRecords<AggregateId<MyAggregate>, PlayEvent<MyAggregate>>, ds: DataSource) {
    System.out.println("Processing Events:")

    val ids = records.map{x -> x.key().value }.distinct()
    val aggregateItems = ds.getForIn(ids)

    val evtMap = HashMap<Int, MutableAggregate<MyAggregate>>()

    records.forEach { record ->

        val aggregateId = record.key().value

        aggregateId.let {
            val mutableAggregate = MutableAggregate(getExistingOrNew(aggregateItems, it))

            val evt = record.value()
            evt.applyTo(mutableAggregate)

            //todo: optimization - prepopulate evtMap with all aggregateId and change evtMap[it].model
            evtMap[it] = mutableAggregate
        }

        System.out.println("""eventType: [${record.value().javaClass.simpleName}] appliedToAggregateId [${record.key().value}] topic: [${record.topic()}] partition: [${record.partition()}] offset: [${record.offset()}]""")
    }

    System.out.println()
    System.out.println("Processing Changes:")

    evtMap.forEach { kvp ->
        System.out.println("""item key: [${kvp.key}] value [${kvp.value}] """)
    }

    ds.upsert(evtMap.values.map{x -> x.model}.toList())
}

fun getExistingOrNew(aggregateItems: List<MyAggregate>, aggregateId: Int): MyAggregate {
    return (aggregateItems.firstOrNull { x -> x.legend.aggregateId.value == aggregateId }) ?: MyAggregate(AggregateLegend(aggregateId, 0), "")
}
