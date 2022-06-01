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
    file_table_params = {"table name": "File",
                         "primary": {"name": "file_id", "type":"INTEGER", "check": "CHECK(id > 0)"},
                         "fields": [
                             {"name": "type", "type":"TEXT","check":"NOT NULL"},
                             {"name": "file_size", "type":"INTEGER", "check":"NOT NULL CHECK(size >= 0)"},
                            ]
                         }
    disk_table_params = {"table name": "Disk",
                         "primary": {"name": "disk_id", "type":"INTEGER", "check": "CHECK(id > 0)"},
                         "fields": [
                             {"name": "company", "type":"TEXT","check":"NOT NULL"},
                             {"name": "speed", "type":"INTEGER", "check":"NOT NULL CHECK(speed > 0)"},
                             {"name": "space", "type":"INTEGER", "check":"NOT NULL CHECK(space >= 0)"},
                             {"name": "cost_per_byte", "type": "INTEGER", "check":"NOT NULL CHECK (cost_per_byte > 0)"}
                            ]
                         }
    ram_table_params = {"table name": "Ram",
                        "primary": "ram_id", "type": "INTEGER", "check": "CHECK(id > 0)",
                        "fields": [
                            {"name": "ram_size", "type": "INTEGER", "check": "NOT NULL CHECK(space > 0)"},
                            {"name": "company", "type": "TEXT", "check": "NOT NULL"}
                            ]
                        }
    files_on_disks_table_params = {"table name": "FilesOnDisks",
                         "primary": {"name": "file_id", "type": "INTEGER", "check": "CHECK(id > 0)",
                                 "name": "disk_id", "type":"INTEGER", "check": "CHECK(id > 0)"},
                         "fields": [
                         ]
                         }
    rams_on_disks_table_params = {"table name": "RamsOnDisks",
                        "primary": {"name": "ram_id", "type": "INTEGER", "check": "CHECK(id > 0)",
                                               "name": "disk_id", "type": "INTEGER", "check": "CHECK(id > 0)"},
                        "fields": [
                                   ]
                        }

    create_table(file_table_params)
    create_table(disk_table_params)
    create_table(ram_table_params)
    create_table(files_on_disks_table_params)
    create_table(rams_on_disks_table_params)

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

def runQuery(query):
    connector = None
    result_query = None
    try:
        connector = Connector.DBConnector()
        result_query = connector.execute(query)
        connector.commit()
    except Exception as e:
        connector.rollback()
        raise e
    finally:
        # will happen any way after try termination or exception handling
        connector.close()
    return result_query


def runCheckQuery(queryString) -> Status:
    try:
        lines, _ = runQuery(queryString)
    except DatabaseException.UNIQUE_VIOLATION:
        return Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return Status.NOT_EXISTS
    except (DatabaseException.NOT_NULL_VIOLATION, DatabaseException.CHECK_VIOLATION):
        return Status.BAD_PARAMS
    except Exception:
        return Status.ERROR
    return Status.OK


def addFile(file: File) -> Status:
    query = sql.SQL("""INSERT INTO File VALUES({id}, {type}, {size})""").format(id=sql.Literal(file.getFileID()),
    type=sql.Literal(file.getType()), size=sql.Literal(file.getSize()))
    return runCheckQuery(query)


def getFileByID(fileID: int) -> File:
    query = sql.SQL("""SELECT * FROM File WHERE id={fileId}""").format(fileId=sql.Literal(fileID))
    try:
        result = runQuery(query)
        # TODO check what is inside result
        return result
    except:
        return File.Badfile()


def deleteFile(file: File) -> Status:
    # TODO Adjust free space on disk if on one disk
    query = sql.SQL("""DELETE FROM File WHERE id={fileId}""").format(fileId=sql.Literal(file.getFileID()))
    return runCheckQuery(query)


def addDisk(disk: Disk) -> Status:
    query = sql.SQL("""INSERT INTO Disk VALUES ({diskId},{company},
    {speed},{space},{cost})""").format(diskId=sql.Literal(disk.getDiskID()),
    company=sql.Literal(disk.getCompany()),speed=sql.Literal(disk.getSpeed()),
    space=sql.Literal(disk.getFreeSpace()),cost=sql.Literal(disk.getCost()))
    return runCheckQuery(query)


def getDiskByID(diskID: int) -> Disk:
    query = sql.SQL("""SELECT * FROM Disk WHERE Disk.id={diskId}""").format(diskId=sql.Literal(diskID))
    try:
        result = runQuery(query)
        # TODO check what is inside result
        return result
    except:
        return Disk.badDisk()


def deleteDisk(diskID: int) -> Status:
    query = sql.SQL("""DELETE FROM Disk WHERE id={diskId}""").format(diskId=sql.Literal(diskID))
    return runCheckQuery(query)


def addRAM(ram: RAM) -> Status:
    query = sql.SQL("""INSERT INTO Ram VALUES ({ramId},{size},{company})""")\
        .format(ramId=sql.Literal(ram.getRamID()),size=sql.Literal(ram.getSize()),company=sql.Literal(ram.getCompany()))
    return runCheckQuery(query)


def getRAMByID(ramID: int) -> RAM:
    query = sql.SQL("""SELECT * FROM Disk WHERE id={ramId}""").format(ramId=sql.Literal(ramID))
    try:
        result = runQuery(query)
        # TODO check what is inside result
        return result
    except:
        return RAM.badRAM()


def deleteRAM(ramID: int) -> Status:
    query = sql.SQL("""DELETE FROM Ram WHERE id={ramId}""").format(ramId=sql.Literal(ramID))
    return runCheckQuery(query)


def addDiskAndFile(disk: Disk, file: File) -> Status:
    query = sql.SQL("""INSERT INTO Disk VALUES({diskId},{company},
       {disk_speed},{disk_space},{disk_cost});
       INSERT INTO File in VALUES ({fileId},{type},{size});""")\
        .format(diskId=sql.Literal(disk.getDiskID()),
    company=sql.Literal(disk.getCompany()),disk_speed=sql.Literal(disk.getSpeed()),
    disk_space=sql.Literal(disk.getFreeSpace()),disk_cost=sql.Literal(disk.getCost()),fileId=sql.Literal(file.getFileID()),
    type=sql.Literal(file.getType()), size=sql.Literal(file.getSize()))
    return runCheckQuery(query)


def addFileToDisk(file: File, diskID: int) -> Status:
    query = sql.SQL("""INSERT INTO FilesOnDisks VALUES({fileId},{diskId}) WHERE disk_id IN (SELECT id AS disk_id FROM Disk WHERE id= {diskId} AND {file_size}<=space) AND id IN;
    UPDATE Disk SET space=space-{file_size} WHERE id={diskId};""").format(diskId=sql.Literal(diskID),fileId=sql.Literal(file.getFileID()),
    file_size=sql.Literal(file.getSize()))
    return runQuery(query)


def removeFileFromDisk(file: File, diskID: int) -> Status:
    query = sql.SQL("""DELETE FROM FilesOnDisks WHERE disk_id = diskId AND file_id = {fileId} ;
        UPDATE Disk SET space=space+{file_size} WHERE id={diskId};""").format(diskId=sql.Literal(diskID),
                                                                              fileId=sql.Literal(file.getFileID()),
                                                                              file_size=sql.Literal(file.getSize()))
    return runCheckQuery(query);


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    query = sql.SQL("""INSERT INTO RamsOnDisks VALUES({ramId},{diskId}) WHERE {diskId} IN (SELECT id FROM Disk WHERE id= {diskId});
    """).format(diskId=sql.Literal(diskID),
    ramId=sql.Literal(ramID))
    return runCheckQuery(query)


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    query = sql.SQL("""DELETE FROM RamsOnDisks WHERE disk_id = diskId AND ram_id = {ramId} ;""")\
        .format(diskId=sql.Literal(diskID),ramId=sql.Literal(ramID))
    return runCheckQuery(query)


def averageFileSizeOnDisk(diskID: int) -> float:
    query = sql.SQL("""SELECT AVG(size) FROM File WHERE id IN (SELECT file_id FROM FilesOnDisks WHERE disk_id = {diskId})""").format(diskId=sql.Literal(diskID))
    result = runQuery(query)
    return result


def diskTotalRAM(diskID: int) -> int:
    query = sql.SQL(
        """SELECT SUM(size) FROM Ram WHERE id IN (SELECT ram_id FROM RamssOnDisks WHERE disk_id = {diskId})""").format(
        diskId=sql.Literal(diskID))
    result = runQuery(query)
    return result


def getCostForType(type: str) -> int:
    # get the disks_id and files_id corresponding INNER JOIN to it the size of the files INNER JOIN
    # the cost per bytes of the disks
    #Multiply the two columns
    query = sql.SQL(
        """SELECT SUM(size*cost_per_byte) FROM (SELECT * FROM FilesOnDisk WHERE file_id in (SELECT id FROM File WHERE type={type_file}) INNER JOIN
         (SELECT id AS file_id,size FROM File WHERE type={type_file}) 
         INNER JOIN (SELECT id AS disk_id,cost_per_byte FROM Disk WHERE id IN 
         (SELECT disk_id FROM FilesOnDisks WHERE file_id IN (SELECT id FROM Files WHERE type={type_file})) ))""").format(
        type_file=sql.Literal(type))
    result = runQuery(query)
    return result


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
