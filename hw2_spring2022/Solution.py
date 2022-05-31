from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


# create the main tables from the parameters.
def create_table(table_params: dict):
    init_query = "BEGIN; CREATE TABLE "
    # add the paramteres to the query and perform it.
    pass


# the difference is the outline of the dictionary, can be in the function above but it is quite confusing to my opinion.
def create_multi_valued_tables(mv_table_params: dict):
    pass


def createTables():
    # creating tables and designing the database
    file_table_params = {"table name": "file",
                         "primary": {"name": "id", "type":"INTEGER", "check": "CHECK(id > 0)"},
                         "fields": [
                             {"name": "type", "type":"TEXT","check":"NOT NULL"},
                             {"name": "size", "type":"INTEGER", "check":"NOT NULL CHECK(size >= 0)"},
                            ]
                         }
    disk_table_params = {"table name": "disk",
                         "primary": {"name": "id", "type":"INTEGER", "check": "CHECK(id > 0)"},
                         "fields": [
                             {"name": "company", "type":"TEXT","check":"NOT NULL"},
                             {"name": "speed", "type":"INTEGER", "check":"NOT NULL CHECK(speed > 0)"},
                             {"name": "space", "type":"INTEGER", "check":"NOT NULL CHECK(space >= 0)"},
                             {"name": "cost_per_byte", "type": "INTEGER", "check":"NOT NULL CHECK (cost_per_byte > 0)"}
                            ]
                         }
    ram_table_params = {"table name": "ram",
                        "primary": "id", "type": "INTEGER", "check": "CHECK(id > 0)",
                        "fields": [
                            {"name": "size", "type": "INTEGER", "check": "NOT NULL CHECK(space > 0)"},
                            {"name": "company", "type": "TEXT", "check": "NOT NULL"}
                            ]
                        }

    create_table(file_table_params)
    create_table(disk_table_params)
    create_table(ram_table_params)

    # the files and ram on the disks are multivalued so we need tables for them, with disk.id as foreign key
    # TODO: understand how it works with CASCADE, FOREIGN KEYS REFERENCES, and ON DELETE
    # TODO: general outline for multivalued dict:
    mv_dict = {"name":"name",
               "primary": "(key1, key2)",
               "foreigns": [{"key":"key_name",
                             "references":"table (field)"}]
            }

    # TODO: do the same thing for VIEW to see how much space left and the total ram, but create query, not table
    pass



def clearTables():
    pass


def dropTables():
    pass


def addFile(file: File) -> Status:
    return Status.OK


def getFileByID(fileID: int) -> File:
    return File()


def deleteFile(file: File) -> Status:
    return Status.OK


def addDisk(disk: Disk) -> Status:
    return Status.OK


def getDiskByID(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> Status:
    return Status.OK


def addRAM(ram: RAM) -> Status:
    return Status.OK


def getRAMByID(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> Status:
    return Status.OK


def addDiskAndFile(disk: Disk, file: File) -> Status:
    return Status.OK


def addFileToDisk(file: File, diskID: int) -> Status:
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    return Status.OK


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def averageFileSizeOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForType(type: str) -> int:
    return 0


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseFiles(fileID: int) -> List[int]:
    return []
