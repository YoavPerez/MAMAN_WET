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
    '''
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
    runQuery(query)
    '''
    return Status.OK


def createTables():
    '''
    # creating tables and designing the database
    query = "BEGIN;"


    # PARAMETERS FOR CREATING THE FILES TABLE
    files_table = {"table name": "File", "fields": [
        {"name": "file_id", "type": "INTEGER", "check": "CHECK (file_id > 0)"},
        {"name": "type", "type": "TEXT", "check": "NOT NULL"},
        {"name": "file_size", "type": "INTEGER", "check": "NOT NULL CHECK (file_size >= 0)"}
    ], "primaries": ["id"]}

    # PARAMETERS FOR CREATING THE DISKS TABLE
    disks_table = {}
    disks_table["table name"] = "Disk"
    disks_table["fields"] = [
                            {"name": "disk_id", "type": "INTEGER", "check": "CHECK (disk_id > 0)"},
                            {"name": "company", "type": "TEXT", "check": "NOT NULL"},
                            {"name": "speed", "type": "INTEGER", "check": "NOT NULL CHECK (speed > 0)"},
                            {"name": "space", "type": "INTEGER", "check": "NOT NULL CHECK (space >= 0)"},
                            {"name": "cost_per_byte", "type": "INTEGER", "check": "NOT NULL CHECK (cost_per_byte > 0)"}
                        ]
    disks_table["primaries"] = ["disk_id"]

    # PARAMETERS FOR CREATING THE RAMS TABLE
    rams_table = {}
    rams_table["table name"] = "Ram"
    rams_table["fields"] = [
                            {"name": "ram_id", "type": "INTEGER", "check": "CHECK (ram_id > 0)"},
                            {"name": "size", "type": "INTEGER", "check": "NOT NULL CHECK (size  > 0)"},
                            {"name": "company", "type": "TEXT", "check": "NOT NULL"}
                        ]

    rams_table["primaries"] = ["ram_id"]

    # PARAMETERS FOR CREATING THE MULTI VALUED FILES FOR DISKS TABLE
    disk_mv_files = {}
    disk_mv_files["table name"] = "FilesOnDisks"
    disk_mv_files["fields"] = [{"name": "file_id", "type": "INTEGER", "check": ""},
                               {"name": "disk_id", "type": "INTEGER", "check": ""}]
    disk_mv_files["primaries"] = ["file_id", "disk_id"]
    disk_mv_files["foreign"] = [{"name": "disk_id", "orig table": "Disk", "orig name": "disk_id"},
                                               {"name": "file_id", "orig table": "File", "orig name": "file_id"}]

    # PARAMETERS FOR CREATING THE MULTI VALUED RAMS FOR DISKS TABLE
    disk_mv_rams = {}
    disk_mv_rams["table name"] = "RamsOnDisks"
    disk_mv_rams["fields"] = [{"name": "ram_id", "type": "INTEGER", "check": ""},
                                            {"name": "disk_id", "type": "INTEGER", "check": ""}]

    disk_mv_rams["primaries"] = ["ram_id", "disk_id"]
    disk_mv_rams["foreign"] = [{"name": "disk_id", "orig table": "Disk", "orig name": "disk_id"},
                                              {"name": "ram_id", "orig table": "Ram", "orig name": "ram_id"}]


    # getting the contribution for the total query from each table
    query += create_table(files_table)
    query += create_table(disks_table)
    query += create_table(rams_table)
    query += create_table(disk_mv_files)
    query += create_table(disk_mv_rams)

    # TODO: add VIEW to help complex calculating

    query += "COMMIT;"
    '''

    query = """BEGIN;
    CREATE TABLE IF NOT EXISTS File
    (file_id INTEGER PRIMARY KEY , 
    type TEXT NOT NULL, 
    file_size INTEGER NOT NULL ,
    CHECK (file_id > 0),
    CHECK (file_size >= 0)
    );
    CREATE TABLE IF NOT EXISTS Disk
    (disk_id INTEGER  PRIMARY KEY CHECK (disk_id > 0), 
    company TEXT NOT NULL, 
    speed INTEGER NOT NULL , 
    space INTEGER NOT NULL , 
    cost_per_byte INTEGER NOT NULL ,
    CHECK (speed > 0),
    CHECK (space >= 0),
    CHECK (cost_per_byte > 0)
    
    );
    CREATE TABLE IF NOT EXISTS Ram
    (ram_id INTEGER  PRIMARY KEY , 
    ram_size INTEGER NOT NULL , 
    company TEXT NOT NULL,
    CHECK (ram_id > 0),
    CHECK (ram_size  > 0)
    );
    CREATE TABLE IF NOT EXISTS FilesOnDisks
    (file_id INTEGER, 
    disk_id INTEGER,
    PRIMARY KEY (file_id,disk_id),
    FOREIGN KEY (disk_id)
     REFERENCES Disk (disk_id)
     ON DELETE CASCADE,
    FOREIGN KEY (file_id)
     REFERENCES File (file_id)
     ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS RamsOnDisks
    (ram_id INTEGER , 
    disk_id INTEGER , 
    PRIMARY KEY (ram_id, disk_id), 
    FOREIGN KEY (disk_id)
     REFERENCES Disk (disk_id)
     ON DELETE CASCADE,
    FOREIGN KEY (ram_id)
     REFERENCES Ram (ram_id)
     ON DELETE CASCADE
    );
   CREATE OR REPLACE VIEW good_disks AS 
        SELECT disk_id, speed, COUNT(file_id) AS count_file FROM Disk LEFT OUTER JOIN File ON(Disk.space >= File.file_size) GROUP BY disk_id;
    
    CREATE OR REPLACE VIEW files_data AS SELECT FilesOndisks.file_id, FilesOnDisks.disk_id, File.type, File.file_size FROM FilesOnDisks 
    LEFT OUTER JOIN File ON FilesOnDisks.file_id=File.file_id;
    
    CREATE OR REPLACE VIEW all_data AS SELECT 
    files_data.file_id, files_data.disk_id, files_data.type, files_data.file_size, Disk.cost_per_byte 
    FROM files_data LEFT OUTER JOIN Disk ON Disk.disk_id=files_data.disk_id;
    
    create or replace view fullfile as
    select File.file_id as fid,
    COALESCE (filesondisks.file_id,0) as fod_id from file full join filesondisks on File.file_id = filesondisks.file_id;

    COMMIT;"""
    runQuery(query)
    return Status.OK


def clearTables():
    query = """BEGIN;
                    DELETE FROM FilesOnDisks CASCADE ;
                    DELETE FROM  RamsOnDisks CASCADE ;
                    DELETE FROM  File CASCADE ;
                    DELETE  FROM  Disk CASCADE ;
                    DELETE FROM  Ram CASCADE ;
                    COMMIT;"""
    return runCheckQuery(query)


def dropTables():
    query = """BEGIN;
                DROP VIEW IF EXISTS fullfile;
                DROP VIEW IF EXISTS all_data;
                DROP VIEW  IF EXISTS files_data;
                DROP VIEW IF EXISTS good_disks;
                DROP TABLE IF EXISTS FilesOnDisks;
                DROP TABLE IF EXISTS RamsOnDisks;
                DROP TABLE IF EXISTS File;
                DROP TABLE IF EXISTS Disk;
                DROP TABLE IF EXISTS Ram;
                COMMIT;"""
    return runCheckQuery(query)


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
    except Exception as e:
        return Status.ERROR
    return Status.OK


def addFile(file: File) -> Status:
    query = sql.SQL("""INSERT INTO File VALUES({id}, {type}, {size})""").format(id=sql.Literal(file.getFileID()),
                                                                                type=sql.Literal(file.getType()),
                                                                                size=sql.Literal(file.getSize()))
    return runCheckQuery(query)


def getFileByID(fileID: int) -> File:
    query = sql.SQL("""SELECT * FROM File WHERE file_id={fileId}""").format(fileId=sql.Literal(fileID))
    try:
        result = runQuery(query)
        # TODO check what is inside result
        Id, kind, size = result[1].rows[0]
        return File(Id, kind, size)
    except Exception:
        return File.badFile()


def deleteFile(file: File) -> Status:
    query = sql.SQL("""UPDATE Disk SET space=space+{file_size} WHERE disk_id IN (SELECT disk_id FROM FilesOnDisks 
    WHERE file_id={fileId});
    DELETE FROM File WHERE file_id={fileId}""").format(fileId=sql.Literal(file.getFileID()),
                                                       file_size=sql.Literal(file.getSize()))
    return runCheckQuery(query)


def addDisk(disk: Disk) -> Status:
    query = sql.SQL("""INSERT INTO Disk VALUES ({diskId},{company},{speed},{space},{cost})""") \
        .format(diskId=sql.Literal(disk.getDiskID()),
                company=sql.Literal(disk.getCompany()), speed=sql.Literal(disk.getSpeed()),
                space=sql.Literal(disk.getFreeSpace()), cost=sql.Literal(disk.getCost()))
    return runCheckQuery(query)


def getDiskByID(diskID: int) -> Disk:
    query = sql.SQL("""SELECT * FROM Disk WHERE disk_id={diskId}""").format(diskId=sql.Literal(diskID))
    try:
        result = runQuery(query)
        # TODO check what is inside result
        Id, company, speed, space, cost = result[1].rows[0]
        return Disk(Id, company, speed, space, cost)
    except:
        return Disk.badDisk()


def deleteDisk(diskID: int) -> Status:
    query = sql.SQL("""DELETE FROM Disk WHERE disk_id={diskId}""").format(diskId=sql.Literal(diskID))
    try:
        result = runQuery(query)
        if result[0] == 0:
            return Status.NOT_EXISTS
        return Status.OK
    except Exception:
        return Status.ERROR



def addRAM(ram: RAM) -> Status:
    query = sql.SQL("""INSERT INTO Ram VALUES ({ramId},{size},{company})""") \
        .format(ramId=sql.Literal(ram.getRamID()), size=sql.Literal(ram.getSize()),
                company=sql.Literal(ram.getCompany()))
    return runCheckQuery(query)


def getRAMByID(ramID: int) -> RAM:
    query = sql.SQL("""SELECT * FROM Ram WHERE ram_id={ramId}""").format(ramId=sql.Literal(ramID))
    try:
        result = runQuery(query)
        # TODO check what is inside result
        Id, size, comp = result[1].rows[0]
        return RAM(Id, comp, size)
    except:
        return RAM.badRAM()


def deleteRAM(ramID: int) -> Status:
    query = sql.SQL("""DELETE FROM Ram WHERE ram_id={ramId}""").format(ramId=sql.Literal(ramID))

    try:
        result = runQuery(query)
        if result[0] == 0:
            return Status.NOT_EXISTS
        return Status.OK
    except Exception:
        return Status.ERROR


def addDiskAndFile(disk: Disk, file: File) -> Status:
    query = sql.SQL("""INSERT INTO Disk VALUES({diskId},{company},
       {disk_speed},{disk_space},{disk_cost});
       INSERT INTO File VALUES ({fileId},{type},{size});""") \
        .format(diskId=sql.Literal(disk.getDiskID()),
                company=sql.Literal(disk.getCompany()), disk_speed=sql.Literal(disk.getSpeed()),
                disk_space=sql.Literal(disk.getFreeSpace()), disk_cost=sql.Literal(disk.getCost()),
                fileId=sql.Literal(file.getFileID()),
                type=sql.Literal(file.getType()), size=sql.Literal(file.getSize()))
    return runCheckQuery(query)


def addFileToDisk(file: File, diskID: int) -> Status:
    query = sql.SQL("""INSERT INTO FilesOnDisks VALUES({fileId},{diskId});
    UPDATE Disk SET space=space-{file_size} WHERE disk_id={diskId} ;
     COMMIT;""").format(diskId=sql.Literal(diskID), fileId=sql.Literal(file.getFileID()),
                        file_size=sql.Literal(file.getSize()))
    return runCheckQuery(query)


def removeFileFromDisk(file: File, diskID: int) -> Status:
    query = sql.SQL("""
    UPDATE Disk SET space=space+{file_size} WHERE disk_id IN (SELECT disk_id FROM FilesOnDisks WHERE file_id={fileId} AND disk_id ={diskId});
    DELETE FROM FilesOnDisks WHERE disk_id = {diskId} AND file_id = {fileId};
     
        """).format(diskId=sql.Literal(diskID),
               fileId=sql.Literal(file.getFileID()),
               file_size=sql.Literal(
                   file.getSize()))
    return runCheckQuery(query);


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    query = sql.SQL("""INSERT INTO RamsOnDisks VALUES({ramId},{diskId});
    """).format(diskId=sql.Literal(diskID),
                ramId=sql.Literal(ramID))
    return runCheckQuery(query)


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    query = sql.SQL("""DELETE FROM RamsOnDisks WHERE disk_id = {diskId} AND ram_id = {ramId} ;""") \
        .format(diskId=sql.Literal(diskID), ramId=sql.Literal(ramID))

    try:
        res = runQuery(query)
        if res[0] == 0: return Status.NOT_EXISTS
        return Status.OK
    except Exception: return Status.ERROR



def averageFileSizeOnDisk(diskID: int) -> float:
    query = sql.SQL(
        """SELECT COALESCE(AVG(file_size),0) FROM File WHERE file_id IN (SELECT file_id FROM FilesOnDisks WHERE disk_id = {diskId})""").format(
        diskId=sql.Literal(diskID))

    try:
        result = runQuery(query)
        return float(result[1].rows[0][0])
    except Exception:
        return -1


def diskTotalRAM(diskID: int) -> int:
    query = sql.SQL(
        """SELECT COALESCE(SUM(ram_size),0) FROM Ram WHERE ram_id IN (SELECT ram_id FROM RamsOnDisks WHERE disk_id = {diskId})""").format(
        diskId=sql.Literal(diskID))

    try:
        result = runQuery(query)
        return int(result[1].rows[0][0])
    except Exception:
        return -1


def getCostForType(type: str) -> int:
    # get the disks_id and files_id corresponding INNER JOIN to it the size of the files INNER JOIN
    # the cost per bytes of the disks
    # Multiply the two columns
    # ce a vw of the files ids that are under that type
    query = sql.SQL(
        """SELECT SUM(all_data.cost_per_byte* all_data.file_size) FROM all_data WHERE all_data.type={type_file};""").format(
        type_file=sql.Literal(type))

    try:
        result = runQuery(query)
        if(result[1].rows[0][0] == None):return 0
        return int(result[1].rows[0][0])
    except Exception:
        return -1


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    query = sql.SQL(
        """SELECT file_id FROM File 
            WHERE file_size <= (SELECT space FROM Disk WHERE disk_id={diskId}) 
           ORDER BY file_id DESC LIMIT 5""").format(
        diskId=sql.Literal(diskID))

    try:
        result = runQuery(query)
        return [tup[0] for tup in result[1].rows]
    except Exception:
        return []



def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    query = sql.SQL(
        """SELECT file_id FROM File WHERE file_size<=(SELECT space FROM DISK WHERE disk_id={diskId}) AND
         file_size<=(SELECT COALESCE (SUM(ram_size), 0) FROM Ram WHERE ram_id IN (SELECT ram_id FROM RamsOnDisks WHERE disk_id={diskId})) 
         ORDER BY file_id ASC LIMIT 5""").format(
        diskId=sql.Literal(diskID))
    try:
        result = runQuery(query)
        return [tup[0] for tup in result[1].rows]
    except Exception:
        return []


def isCompanyExclusive(diskID: int) -> bool:
    # TODO transform to bool value
    query = sql.SQL(
        """ SELECT company FROM Disk WHERE disk_id={diskId} AND 
        company=ALL(SELECT company FROM Ram WHERE ram_id IN (SELECT ram_id FROM RamsOnDisks WHERE disk_id={diskId}));
        """).format(
        diskId=sql.Literal(diskID))
    try:
        result = runQuery(query)
        return result[0] == 1
    except Exception:
        return False


def getConflictingDisks() -> List[int]:
    query = sql.SQL(
        """SELECT DISTINCT F1.disk_id FROM FilesOnDisks F1, FilesOnDisks F2 
        WHERE F1.disk_id != F2.disk_id AND F1.file_id = F2.file_id ORDER BY F1.disk_id ASC
        """)

    try :
        result = runQuery(query)
        return [tup[0] for tup in result[1].rows]
    except:
        return []


def mostAvailableDisks() -> List[int]:
    query = sql.SQL(
        """SELECT disk_id FROM good_disks
		ORDER BY count_file DESC, speed DESC,disk_id ASC
        LIMIT 5;""")
    try:
        result = runQuery(query)[1].rows
        return [tup[0] for tup in result]
    except Exception:
        return []

def getCloseFiles(fileID: int) -> List[int]:
    query = sql.SQL(
        """
        
  (SELECT DISTINCT D2.file_id as file_id FROM FilesOnDisks D1,FilesOnDisks D2 
    WHERE (D1.disk_id=D2.disk_id AND D1.file_id={fileId} AND D2.file_id!={fileId}) OR 0=(SELECT COUNT(file_id) FROM FilesOnDisks WHERE file_id={fileId})
    GROUP BY D2.file_id HAVING (COUNT(D2.disk_id))>=(0.5*(SELECT COUNT(disk_id) FROM FilesOnDisks WHERE file_id={fileId}) 
))
UNION(
	(select x1.fid  from fullfile x1,fullfile x2 where x1.fid != {fileId} And x1.fod_id = 0 And x2.fid = {fileId} and x2.fod_id = 0 ))
	INTERSECT 
	 (SELECT file_id FROM File WHERE file_id IN 
	 (SELECT F1.file_id FROM File F1,File F2 WHERE F1.file_id!={fileId} AND F2.file_id={fileId})) 	    
ORDER BY file_id
         LIMIT 10""").format(
        fileId=sql.Literal(fileID))
    try:
        result = runQuery(query)[1].rows
        return [tup[0] for tup in result]

    except Exception:
        return []



####### test create table #######
if __name__ == "__main__":
    conn = None
    try:
        dropTables()
        # createTables()
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
        print("bye")
