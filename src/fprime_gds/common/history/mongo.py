import datetime
from fprime_gds.common.templates.ch_template import ChTemplate
from pymongo import MongoClient
from fprime_gds.common.data_types.ch_data import ChData
from fprime_gds.common.history.history import History
from fprime.common.models.serialize import time_type
from fprime.common.models.serialize.type_base import DictionaryType


class MongoDBHistory(History):
    def __init__(self, mongo_client: MongoClient, database_name: str, dict_name):
        """
        Constructor used to set-up MongoDB connection and database for history

        :param mongo_client: MongoClient instance
        :param database_name: Name of the MongoDB database
        """
        self.dict_name=dict_name
        self.client = mongo_client
        self.database = self.client[database_name]
        self.last_time = datetime.datetime.now()
        self.all_last_time = {}
        

    def data_callback(self, data:ChData, sender=None):
        """
        Data callback to store

        :param data: object to store
        :param channel_id: channel id for the data
        """
        collection_name = f"{data.get_template().get_full_name()}"
        if collection_name not in self.database.list_collection_names():
            self.database.create_collection(collection_name)
        collection = self.database[collection_name]

        time:time_type.TimeType =data.time

        document = {
            "time": time.get_datetime(),
            "dict": data.get_val()
        }
        collection.insert_one(document)

    def retrieve(self, limit=None, id=None, start=None, end=None):
        """
        Retrieve objects from MongoDB based on channel id and timestamps

        :param channel_id: Channel id to filter the data
        :param start: Start timestamp
        :param end: End timestamp
        :return: a list of ChData objects matching the query
        """
        # It behave as retrieve new

        the_end = datetime.datetime.now()
        start=self.last_time

        result=[]

        for collection_name in self.database.list_collection_names():
            query = {}
            if(collection_name in self.all_last_time):
                start = self.all_last_time[collection_name]
            query["time"] = {"$gte": start, "$lt": the_end}
            collection = self.database[collection_name]
            cursor = collection.find(query)

            data_found=False
            for doc in cursor:
                if(collection_name in self.dict_name):
                    ch_template:ChTemplate =self.dict_name[collection_name]
                    my_obj = ch_template.get_type_obj()()
                    my_obj.val = doc["dict"]
                    my_time = time_type.TimeType()
                    my_time.set_datetime(doc["time"])
                    ch_data= ChData(my_obj, my_time, self.dict_name[collection_name])
                    result.append(ch_data)
                    data_found=True
            if(data_found):
                self.all_last_time[collection_name] = the_end
        return result

    def size(self):
        """
        Accessor for the number of objects in the history based on channel id and timestamps

        :param channel_id: Channel id to filter the data
        :param start: Start timestamp
        :param end: End timestamp
        :return: the number of objects (int)
        """
        # The history has technically zero size since everything is stored in the db
        return 0
    
    def retrieve_new(self):
        """
        Retrieves a chronological order of objects that haven't been accessed through retrieve or
        retrieve_new before.

        Returns:
            a list of objects in chronological order
        """
        # For now retrieve behave as retrieve new
        return self.retrieve()

    def clear(self, start=None):
        """
        Clears objects from RamHistory. It clears upto the earliest session. If session is supplied, the session id will
        be deleted as well.

        Args:
            start: a position in the history's order (int).
        """

        return