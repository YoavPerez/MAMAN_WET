from typing import List
import hw2_spring2022.Utility.DBConnector as Connector
from hw2_spring2022.Utility.Status import Status
from hw2_spring2022.Utility.Exceptions import DatabaseException
from hw2_spring2022.Business.File import File
from hw2_spring2022.Business.RAM import RAM
from hw2_spring2022.Business.Disk import Disk
from psycopg2 import sql


# create the main tables from the parameters.
def create_table(table_params: dict):
    query = "CREATE TABLE "
    # add the parameters to the query and perform it.
    query += table_params["table name"] + "\n("

    for field in table_params["fields"]:
        field_line = "{name} {kind} {check}, \n".format(name=field["name"], kind=field["type"], check=field["check"])
        query += field_line

    query += "PRIMARY KEY ("
    for i, prime in enumerate(table_params["primaries"]):
        if i < len(table_params["primaries"]) - 1:
            query += prime + ", "
        else:
            query += prime
    query += ")"

    if "foreign" in table_params.keys():
        query += ", \n"
        for i, fk in enumerate( table_params["foreign"]):

            query += "FOREIGN KEY ({key})\n REFERENCES {table} ({orig_f})\n ON DELETE CASCADE".format(
                key=fk["name"], table=fk["orig table"], orig_f=fk["orig name"]
            )
            if i < len(table_params["foreign"])-1:
                query += ",\n"
            else:
                query += "\n"
    else: query += "\n"
    query += ");\n"
    return query


# the difference is the outline of the dictionary, can be in the function above but it is quite confusing to my opinion.
def create_multi_valued_tables(mv_table_params: dict):
    pass


def createTables():
    # creating tables and designing the database
    query = "BEGIN;"

    # PARAMETERS FOR CREATING THE FILES TABLE
    files_table = {"table name": "File", "fields": [
        {"name": "id", "type": "INTEGER", "check": "CHECK (id > 0)"},
        {"name": "type", "type": "TEXT", "check": "NOT NULL"},
        {"name": "size", "type": "INTEGER", "check": "NOT NULL CHECK (size >= 0)"}
    ], "primaries": ["id"]}

    # PARAMETERS FOR CREATING THE DISKS TABLE
    disks_table = {}
    disks_table["table name"] = "Disk"
    disks_table["fields"] = [
                            {"name": "id", "type": "INTEGER", "check": "CHECK (id > 0)"},
                            {"name": "company", "type": "TEXT", "check": "NOT NULL"},
                            {"name": "speed", "type": "INTEGER", "check": "NOT NULL CHECK (speed > 0)"},
                            {"name": "space", "type": "INTEGER", "check": "NOT NULL CHECK (space >= 0)"},
                            {"name": "cost_per_byte", "type": "INTEGER", "check": "NOT NULL CHECK (cost_per_byte > 0)"}
                        ]
    disks_table["primaries"] = ["id"]

    # PARAMETERS FOR CREATING THE RAMS TABLE
    rams_table = {}
    rams_table["table name"] = "Ram"
    rams_table["fields"] = [
                            {"name": "id", "type": "INTEGER", "check": "CHECK (id > 0)"},
                            {"name": "size", "type": "INTEGER", "check": "NOT NULL CHECK (size  > 0)"},
                            {"name": "company", "type": "TEXT", "check": "NOT NULL"}
                        ]
    rams_table["primaries"] = ["id"]

    # PARAMETERS FOR CREATING THE MULTI VALUED FILES FOR DISKS TABLE
    disk_mv_files = {}
    disk_mv_files["table name"] = "FilesOnDisks"
    disk_mv_files["fields"] = [{"name": "file_id", "type": "INTEGER", "check": ""},
                               {"name": "disk_id", "type": "INTEGER", "check": ""}]
    disk_mv_files["primaries"] = ["file_id", "disk_id"]
    disk_mv_files["foreign"] = [{"name": "disk_id", "orig table": "Disk", "orig name": "id"},
                                               {"name": "file_id", "orig table": "File", "orig name": "id"}]

    # PARAMETERS FOR CREATING THE MULTI VALUED RAMS FOR DISKS TABLE
    disk_mv_rams = {}
    disk_mv_rams["table name"] = "RamsOnDisks"
    disk_mv_rams["fields"] = [{"name": "ram_id", "type": "INTEGER", "check": ""},
                                            {"name": "disk_id", "type": "INTEGER", "check": ""}]
    
    disk_mv_rams["primaries"] = ["ram_id", "disk_id"]
    disk_mv_rams["foreign"] = [{"name": "disk_id", "orig table": "Disk", "orig name": "id"},
                                              {"name": "ram_id", "orig table": "Ram", "orig name": "id"}]
    

    # getting the contribution for the total query from each table
    query += create_table(files_table)
    query += create_table(disks_table)
    query += create_table(rams_table)
    query += create_table(disk_mv_files)
    query += create_table(disk_mv_rams)

    # TODO: add VIEW to help complex calculating

    query += "COMMIT;"
    return query

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
                                                                                type=sql.Literal(file.getType()),
                                                                                size=sql.Literal(file.getSize()))
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
                                       company=sql.Literal(disk.getCompany()), speed=sql.Literal(disk.getSpeed()),
                                       space=sql.Literal(disk.getFreeSpace()), cost=sql.Literal(disk.getCost()))
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
    query = sql.SQL("""INSERT INTO Ram VALUES ({ramId},{size},{company})""") \
        .format(ramId=sql.Literal(ram.getRamID()), size=sql.Literal(ram.getSize()),
                company=sql.Literal(ram.getCompany()))
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
       INSERT INTO File in VALUES ({fileId},{type},{size});""") \
        .format(diskId=sql.Literal(disk.getDiskID()),
                company=sql.Literal(disk.getCompany()), disk_speed=sql.Literal(disk.getSpeed()),
                disk_space=sql.Literal(disk.getFreeSpace()), disk_cost=sql.Literal(disk.getCost()),
                fileId=sql.Literal(file.getFileID()),
                type=sql.Literal(file.getType()), size=sql.Literal(file.getSize()))
    return runCheckQuery(query)


def addFileToDisk(file: File, diskID: int) -> Status:
    query = sql.SQL("""INSERT INTO FilesOnDisks VALUES({fileId},{diskId}) WHERE id IN (SELECT id FROM Disk WHERE id= {diskId} AND {file_size}<=space);
    UPDATE Disk SET space=space-{file_size} WHERE id={diskId};""").format(diskId=sql.Literal(diskID),
                                                                          fileId=sql.Literal(file.getFileID()),
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
    query = sql.SQL("""DELETE FROM RamsOnDisks WHERE disk_id = diskId AND ram_id = {ramId} ;""") \
        .format(diskId=sql.Literal(diskID), ramId=sql.Literal(ramID))
    return runCheckQuery(query)


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

if __name__ == "__main__":
    conn = None
    try:
        conn = Connector.DBConnector()
        q = createTables()
        print(q)
        conn.execute(q)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()