package my.artifact.myeventplayer.consumer.datasource

import my.artifact.myeventplayer.common.aggregate.MyAggregate

class DataSource() {

    fun upsert(items: List<MyAggregate>) {
        //todo: create upsert dat asource implementation
    }

    fun getForIn(ids: List<Int>): List<MyAggregate> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

}

