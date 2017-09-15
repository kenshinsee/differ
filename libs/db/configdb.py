import os
import connectionmanager as cn
from sqlalchemy import Column, Integer, String, DateTime, Boolean, func, PrimaryKeyConstraint, ForeignKey


class ConfigHistory(cn.Connections.base):
    __tablename__ = 'CONFIG_HISTORY'
    CONFIG_ID = Column(Integer, primary_key=True, nullable=False)
    CONFIG_TABLE = Column(String(512), nullable=False)
    CONFIG_COLUMN = Column(String(512), nullable=False)
    OLD_VALUE = Column(String(512), nullable=True)
    NEW_VALUE = Column(String(512), nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class FileMoverServer(cn.Connections.base):
    __tablename__ = 'FILEMOVER_SERVER'
    NAME = Column(String(512), primary_key=True, nullable=False)
    TYPE = Column(String(10), nullable=False)
    ADDRESS = Column(String(512), nullable=True)
    PORT = Column(Integer, nullable=True)
    USERNAME = Column(String(512), nullable=True)
    PASSWORD = Column(String(512), nullable=True)
    ROOT_DIRECTORY = Column(String(512), nullable=True)
    UPLOAD_DIRECTORY = Column(String(512), nullable=True)
    INPROGRESS_UPLOAD_DIRECTORY = Column(String(512), nullable=True)
    RENAME = Column(Boolean, nullable=False)
    ACTIVE = Column(Boolean, nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class FileMoverMapping(cn.Connections.base):
    __tablename__ = 'FILEMOVER_MAPPING'
    FILE_MOVER_MAPPING_ID = Column(Integer, primary_key=True, nullable=False)
    SOURCE_SERVER = Column(String(512), nullable=False)
    SOURCE_PATH = Column(String(512), nullable=False)
    TARGET_SERVER = Column(String(512), nullable=False)
    FILE_PATTERN = Column(String(512), nullable=False)
    KEEP_SUBFOLDER = Column(Boolean, nullable=False)
    DELETE_SOURCE = Column(Boolean, nullable=False)
    PRIORITY = Column(Integer, nullable=False)
    BACKUP_FOLDER = Column(String(512), nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class FileMoverAvailableFiles(cn.Connections.base):
    __tablename__ = 'FILEMOVER_AVAILABLEFILES'
    FILE_ID = Column(Integer, primary_key=True, nullable=False)
    SERVER_NAME = Column(String(512), ForeignKey("FILEMOVER_SERVER.NAME"), nullable=False)
    SOURCE_PATH = Column(String(512), nullable=False)
    FULL_PATH = Column(String(512), nullable=False)
    FILE_SIZE = Column(Integer, nullable=False)
    FILE_LAST_MODIFIED = Column(DateTime(timezone=True), nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class FileMoverStatus(cn.Connections.base):
    __tablename__ = 'FILEMOVER_STATUS'
    FILE_MOVER_STATUS_ID = Column(Integer, primary_key=True)
    FILE_ID = Column(Integer, nullable=False)
    TARGET_SERVER = Column(String(512), nullable=False)
    STATUS = Column(String(64), nullable=False)
    STATUS_REASON = Column(String(1024), nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class DataStreamRawTableDefinition(cn.Connections.base):
    __tablename__ = 'DATASTREAM_RAW_TABLE_DEFINITION'
    RAW_TABLE = Column(String(512), primary_key=True, nullable=False)
    COLUMN_NAME = Column(String(512), primary_key=True, nullable=False)
    COLUMN_DATA_TYPE = Column(String(512), nullable=False)
    COLUMN_DATA_STYLE = Column(String(512), nullable=True)
    SEGMENTATION_POSITION = Column(Integer, nullable=True)
    ORDER_BY_POSITION = Column(Integer, nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    __table_args__ = (PrimaryKeyConstraint('RAW_TABLE', 'COLUMN_NAME'),)


class DataStreamSourceFolder(cn.Connections.base):
    __tablename__ = 'DATASTREAM_SOURCE_FOLDER'
    SOURCE_FOLDER = Column(String(512), primary_key=True, nullable=False)
    VENDOR_KEY = Column(Integer, nullable=True)
    RETAILER_KEY = Column(Integer, nullable=True)
    DC_RETAILER_KEY = Column(Integer, nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class DataStreamFileFormat(cn.Connections.base):
    __tablename__ = 'DATASTREAM_FILE_FORMAT'
    FORMAT_ID = Column(Integer, primary_key=True, nullable=False)
    FORMAT_NAME = Column(String(256), nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class DataStreamFileHeader(cn.Connections.base):
    __tablename__ = 'DATASTREAM_FILE_HEADER'
    HEADER_ID = Column(Integer, primary_key=True, nullable=False)
    FORMAT_ID = Column(Integer, nullable=False)
    FILE_PATTERN = Column(String(512), nullable=False)
    ARCHIVE_FILE_PATTERN = Column(String(512), nullable=False)
    SOURCE_FOLDER = Column(String(512), nullable=True)
    PATTERN_PRIORITY = Column(Integer, nullable=True)
    PROCESS_PRIORITY = Column(Integer, nullable=True)
    DATASTREAM = Column(String(512), nullable=False)
    DATASTREAM_SUB = Column(String(512), nullable=False)
    DELIMITER = Column(String(16), nullable=False)
    HEADER_ROW = Column(Integer, nullable=False)
    DATA_ROW = Column(Integer, nullable=True)
    HAS_HEADER = Column(Boolean, nullable=False)
    VERIFY_THRESHOLD = Column(Integer, nullable=False)
    ERROR_THRESHOLD = Column(Integer, nullable=False)
    IF_NEW_COLUMN = Column(String(16), nullable=False)
    RAW_TABLE = Column(String(512), nullable=False)
    VENDOR_KEY = Column(Integer, nullable=True)
    RETAILER_KEY = Column(Integer, nullable=True)
    DC_RETAILER_KEY = Column(Integer, nullable=True)
    POST_SCRIPT = Column(String(256), nullable=True)
    ACTIVE = Column(Boolean, nullable=False)
    WHSE = Column(Boolean, nullable=False)
    IGNORE_BLANK_ROWS = Column(Boolean, nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class DataStreamFileLayout(cn.Connections.base):
    __tablename__ = 'DATASTREAM_FILE_LAYOUT'
    FILE_LAYOUT_ID = Column(Integer, primary_key=True)
    FORMAT_ID = Column(Integer, ForeignKey("DATASTREAM_FILE_FORMAT.FORMAT_ID"), nullable=False)
    FILE_COLUMN_NAME = Column(String(512), nullable=False)
    FILE_COLUMN_ORDER = Column(Integer, nullable=True)
    FILE_COLUMN_FIXED_LENGTH = Column(Integer, nullable=True)
    RAW_COLUMN_NAME = Column(String(512), nullable=True)
    IF_MISSING = Column(String(16), nullable=False)
    IF_CONTAINS_NULL = Column(String(16), nullable=False)
    IGNORE = Column(Boolean, nullable=False)
    CONVERT_DATA_TYPE = Column(String(512), nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class DataStreamFileAttribute(cn.Connections.base):
    __tablename__ = 'DATASTREAM_FILE_ATTRIBUTE'
    FILE_ATTRIBUTE_ID = Column(Integer, primary_key=True)
    HEADER_ID = Column(Integer, ForeignKey("DATASTREAM_FILE_HEADER.HEADER_ID"), nullable=False)
    VENDOR_KEY = Column(Integer, nullable=True)
    RETAILER_KEY = Column(Integer, nullable=True)
    DC_RETAILER_KEY = Column(Integer, nullable=True)
    NAME = Column(String(255), nullable=False)
    VALUE = Column(String(255), nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class DataStreamAudit(cn.Connections.base):
    __tablename__ = 'DATASTREAM_AUDIT'
    LOAD_ID = Column(Integer, primary_key=True, nullable=False)
    HEADER_ID = Column(Integer, nullable=True)
    FORMAT_ID = Column(Integer, nullable=True)
    FILE_NAME = Column(String(512), nullable=False)
    ARCHIVE_NAME = Column(String(512), nullable=True)
    SOURCE_FOLDER = Column(String(512), nullable=True)
    DATASTREAM = Column(String(512), nullable=True)
    DATASTREAM_SUB = Column(String(512), nullable=True)
    STATUS = Column(String(64), nullable=False)
    STATUS_REASON = Column(String(1024), nullable=True)
    REVIEW = Column(String(64), nullable=True)
    ROW_COUNT = Column(Integer, nullable=True)
    ROW_ERROR_COUNT = Column(Integer, nullable=True)
    FILE_SIZE = Column(Integer, nullable=True)
    MD5SUM = Column(String(32), nullable=True)
    ARRIVAL_DATE = Column(DateTime(timezone=True), nullable=False)
    ARCHIVE_ARRIVAL_DATE = Column(DateTime(timezone=True), nullable=True)
    LOAD_START_DATE = Column(DateTime(timezone=True), nullable=True)
    LOAD_COMPLETE_DATE = Column(DateTime(timezone=True), nullable=True)
    VENDOR_KEY = Column(Integer, nullable=True)
    RETAILER_KEY = Column(Integer, nullable=True)
    DC_RETAILER_KEY = Column(Integer, nullable=True)
    RAW_TABLE = Column(String(512), nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class DataStreamAuditAttribute(cn.Connections.base):
    __tablename__ = 'DATASTREAM_AUDIT_ATTRIBUTE'
    LOAD_ID = Column(Integer, primary_key=True, nullable=False)
    NAME = Column(String(255), primary_key=True, nullable=False)
    VALUE = Column(String(255), nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class TransformerEvent(cn.Connections.base):
    __tablename__ = 'TRANSFORMER_EVENT'
    EVENT_KEY = Column(Integer, primary_key=True, nullable=False)
    VENDOR_KEY = Column(Integer, nullable=True)
    RETAILER_KEY = Column(Integer, nullable=True)
    DC_RETAILER_KEY = Column(Integer, nullable=True)
    TRANSFORMER = Column(String(512), nullable=True)
    DATASTREAM = Column(String(512), nullable=False)
    STATUS = Column(String(512), nullable=False)
    STATUS_REASON = Column(String(1024), nullable=True)
    TRFM_START_DATE = Column(DateTime(timezone=True), nullable=True)
    TRFM_COMPLETE_DATE = Column(DateTime(timezone=True), nullable=True)
    JOB_NUMBER = Column(Integer, nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class TransformerConflictList(cn.Connections.base):
    __tablename__ = 'TRANSFORMER_CONFLICT_LIST'
    CONFLICT_LIST_ID = Column(Integer, primary_key=True)
    TRANSFORMER = Column(String(512), nullable=False)
    CONFLICT_TRANSFORMER = Column(String(512), nullable=False)
    SAME_VENDOR = Column(Boolean, nullable=True)
    SAME_RETAILER = Column(Boolean, nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class EventFileMapping(cn.Connections.base):
    __tablename__ = 'EVENT_FILE_MAPPING'
    ID = Column(Integer, primary_key=True, nullable=False)
    EVENT_KEY = Column(Integer, nullable=False)
    LOAD_ID = Column(Integer, nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class ApplicationRegistry(cn.Connections.base):
    __tablename__ = 'APPLICATION_REGISTRY'
    APPLICATION_ID = Column(String(512), primary_key=True, nullable=False)
    CONSUMER_NAME = Column(String(512), primary_key=True, nullable=False)
    FACT_TYPE = Column(String(28), nullable=False)
    RETAILER_KEY = Column(Integer, nullable=True)
    VENDOR_KEY = Column(Integer, nullable=True)
    DC_RETAILER_KEY = Column(Integer, nullable=True)
    REGISTER_DATE = Column(DateTime(timezone=True), nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    __table_args__ = (PrimaryKeyConstraint('APPLICATION_ID', 'CONSUMER_NAME'),)


class ApplicationTransfers(cn.Connections.base):
    __tablename__ = 'APPLICATION_TRANSFERS'
    APPLICATION_ID = Column(String(512), primary_key=True, nullable=False)
    CONSUMER_NAME = Column(String(512), primary_key=True, nullable=False)
    EVENT_KEY = Column(Integer, nullable=False)
    STATUS = Column(String(64), nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    __table_args__ = (PrimaryKeyConstraint('APPLICATION_ID', 'CONSUMER_NAME'),)


class FactTypeDataStreamMapping(cn.Connections.base):
    __tablename__ = 'FACT_TYPE_DATASTREAM_MAPPING'
    FACT_TYPE = Column(String(512), primary_key=True, nullable=False)
    DATASTREAM_SUB = Column(String(512), primary_key=True, nullable=False)
    TRANSFER_TYPE = Column(String(512), nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    __table_args__ = (PrimaryKeyConstraint('FACT_TYPE', 'DATASTREAM_SUB'),)


class FactUpdateEvent(cn.Connections.base):
    __tablename__ = 'FACT_UPDATE_EVENT'
    UPDATE_EVENT_ID = Column(Integer, primary_key=True)
    EVENT_KEY = Column(Integer, nullable=False)
    PERIOD_KEY = Column(Integer, nullable=True)
    VENDOR_KEY = Column(Integer, nullable=True)
    RETAILER_KEY = Column(Integer, nullable=True)
    FACT_TYPE = Column(String(28), nullable=False)
    REFERENCE_TYPE = Column(String(28), nullable=False)
    OPERATION_TYPE = Column(String(28), nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class FactTableLookUp(cn.Connections.base):
    __tablename__ = 'FACT_TABLE_LOOKUP'
    LOOKUP_ID = Column(Integer, primary_key=True)
    FACT_TYPE = Column(String(28), nullable=False)
    VENDOR_KEY = Column(Integer, nullable=True)
    RETAILER_KEY = Column(Integer, nullable=True)
    DC_RETAILER_KEY = Column(Integer, nullable=True)
    TABLE_NAME = Column(String(512), nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class GrouperFileset(cn.Connections.base):
    __tablename__ = 'GROUPER_FILESET'
    FILE_SET = Column(String(512), primary_key=True, nullable=False)
    DATASTREAM_SUB = Column(String(512), primary_key=True, nullable=False)
    FILE_TYPE = Column(String(512), nullable=True)
    RETAIL_LINK_ID = Column(String(512), nullable=True)
    REQUIRED = Column(Boolean, nullable=False)
    ACTIVE = Column(Boolean, nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    __table_args__ = (PrimaryKeyConstraint('FILE_SET', 'DATASTREAM_SUB'),)


class GrouperTimeWindow(cn.Connections.base):
    __tablename__ = 'GROUPER_TIMEWINDOW'
    WINDOW_NAME = Column(String(512), primary_key=True, nullable=False)
    VENDOR_KEY = Column(Integer, nullable=True)
    RETAILER_KEY = Column(Integer, nullable=True)
    DC_RETAILER_KEY = Column(Integer, nullable=True)
    TRANSFORMER = Column(String(512), nullable=False)
    SERVICE_NAME = Column(String(512), nullable=True)
    GROUP_TYPE = Column(String(1), nullable=True)
    FACT_UPDATE_TYPE = Column(String(1), nullable=True)
    LOAD_IMMEDIATELY = Column(Boolean, nullable=False)
    START_TIME = Column(DateTime(timezone=True), nullable=True)
    CUTOFF_TIME = Column(Integer, nullable=True)
    DEADLINE_TIME = Column(Integer, nullable=True)
    ADDITIONAL_CHECK_TIMES = Column(String(512), nullable=True)
    TIME_ZONE = Column(String(512), nullable=True)
    ACTIVE = Column(Boolean, nullable=False)
    ENABLE_FILE_REQUIREMENT = Column(Boolean, nullable=True)
    FREQUENCY = Column(String(1), nullable=False)
    EVERY_X_WEEKS = Column(Integer, nullable=True)
    WEEKDAY = Column(String(128), nullable=True)
    EMAIL_NOTIFY_TYPE = Column(String(128), nullable=True)
    ENABLE_FORCE_CUTOFF = Column(Boolean, nullable=True)
    SINGLE_EVENT_PER_DAY = Column(Boolean, nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class GrouperFileSetTimeWindow(cn.Connections.base):
    __tablename__ = 'GROUPER_FILESET_TIMEWINDOW'
    FILE_SET = Column(String(512), primary_key=True, nullable=False)
    WINDOW_NAME = Column(String(512), primary_key=True, nullable=False)
    FILTER_DAYS = Column(Integer, nullable=True)
    FILE_FILTER_DAYS = Column(Integer, nullable=True)
    EXP_FILE_COUNT = Column(Integer, nullable=True)
    MIN_EXP_FILE_COUNT = Column(Integer, nullable=True)
    MAX_EXP_FILE_COUNT = Column(Integer, nullable=True)
    SIZE_METRIC = Column(String(1), nullable=True)
    AUTO_CALC_AVG = Column(Boolean, nullable=True)
    MIN_EXP_FILE_SIZE = Column(Integer, nullable=True)
    MAX_EXP_FILE_SIZE = Column(Integer, nullable=True)
    FILE_SIZE_FLAG = Column(String(1), nullable=False)
    OSM_DEPENDECY = Column(Boolean, nullable=True)
    OSM_EXPECTED_LAG_DAYS = Column(Integer, nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    __table_args__ = (PrimaryKeyConstraint('FILE_SET', 'WINDOW_NAME'),)


class GrouperStatistics(cn.Connections.base):
    __tablename__ = 'GROUPER_STATISTICS'
    FILE_SET = Column(String(512), primary_key=True, nullable=False)
    WINDOW_NAME = Column(String(512), primary_key=True, nullable=False)
    CURRENT_FILE_SIZE = Column(Integer, nullable=False)
    CURRENT_ROW_COUNT = Column(Integer, nullable=False)
    AVERAGE_FILE_SIZE = Column(Integer, nullable=True)
    AVERAGE_ROW_COUNT = Column(Integer, nullable=True)
    FILES_IN_AVG = Column(Integer, nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    __table_args__ = (PrimaryKeyConstraint('FILE_SET', 'WINDOW_NAME'),)


class GrouperAudit(cn.Connections.base):
    __tablename__ = 'GROUPER_AUDIT'
    ID = Column(Integer, primary_key=True, nullable=False)
    WINDOW_NAME = Column(String(512), nullable=False)
    EVENT_KEY = Column(Integer, nullable=True)
    ARE_ALL_OPTIONAL_FILES_AVAILABLE = Column(Boolean, nullable=True)
    ARE_ALL_REQUIRED_FILES_AVAILABLE = Column(Boolean, nullable=True)
    FILES_ARRIVED_AFTER_LAST_STATUS_CHANGE = Column(Integer, nullable=True)
    FILE_REQUIREMENT_MET_BEFORE_CUTOFF = Column(Boolean, nullable=True)
    FILE_REQUIREMENT_MET_DATE = Column(DateTime(timezone=True), nullable=True)
    FORCE_CUTOFF_DONE = Column(Boolean, nullable=True)
    FORCE_CUTOFF_DATE = Column(DateTime(timezone=True), nullable=True)
    ARE_ALL_OPTIONAL_FILES_SIZE_CORRECT = Column(Boolean, nullable=True)
    ARE_ALL_REQUIRED_FILE_SIZE_CORRECT = Column(Boolean, nullable=True)
    LAST_ADDITIONAL_CHECK_DATE = Column(DateTime(timezone=True), nullable=True)
    LAST_ADDITIONAL_CHECK_OUTCOME = Column(String(512), nullable=True)
    CUTOFF_CHECK_DATE = Column(DateTime(timezone=True), nullable=True)
    CUTOFF_CHECK_OUTCOME = Column(String(512), nullable=True)
    DEADLINE_CHECK_DATE = Column(DateTime(timezone=True), nullable=True)
    DEADLINE_CHECK_OUTCOME = Column(String(512), nullable=True)
    STARTTIME_CHECK_DATE = Column(DateTime(timezone=True), nullable=True)
    STARTTIME_CHECK_OUTCOME = Column(String(512), nullable=True)
    LAST_CHECK_DATE = Column(DateTime(timezone=True), nullable=True)
    LAST_CHECK_OUTCOME = Column(String(512), nullable=True)
    BETWEEN_START_CUTOFF = Column(Boolean, nullable=True)
    BETWEEN_START_DEADLINE = Column(Boolean, nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class GrouperTmpTable(cn.Connections.base):
    __tablename__ = 'GrouperTmpTable'
    GROUPER_TMP_ID = Column(Integer, primary_key=True)
    VENDOR_KEY = Column(Integer, nullable=False)
    RETAILER_KEY = Column(Integer, nullable=False)
    DC_RETAILER_KEY = Column(Integer, nullable=True)
    LOAD_ID = Column(Integer, nullable=True)
    DATASTREAM = Column(String(512), nullable=True)
    DATASTREAM_SUB = Column(String(512), nullable=True)
    REQUIRED = Column(Boolean, nullable=True)
    TRANSFORMER = Column(String(512), nullable=True)
    WINDOW_NAME = Column(String(512), nullable=True)
    FILE_SET = Column(String(512), nullable=True)
    EXP_FILE_COUNT = Column(Integer, nullable=True)
    MIN_EXP_FILE_COUNT = Column(Integer, nullable=True)


class FactTypeMapping(cn.Connections.base):
    __tablename__ = 'FACT_TYPE_MAPPING'
    FACT_TYPE = Column(String(100), primary_key=True, nullable=False)
    FACT_IDENTIFIER = Column(String(100), primary_key=True, nullable=False)
    ORG_KEY = Column(Integer, nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    __table_args__ = (PrimaryKeyConstraint('FACT_TYPE', 'FACT_IDENTIFIER'),)


class TransformerFileItemKey(cn.Connections.base):
    __tablename__ = 'TRANSFORMER_FILE_ITEM_KEY'
    FILE_ITEM_ID = Column(Integer, primary_key=True)
    LOAD_ID = Column(Integer, nullable=False)
    EVENT_KEY = Column(Integer, nullable=True)
    FILE_NAME = Column(String(512), nullable=True)
    VENDOR_KEY = Column(Integer, nullable=True)
    RETAILER_KEY = Column(Integer, nullable=True)
    DATASTREAM_SUB = Column(String(512), nullable=True)
    REQUEST_DATE = Column(DateTime(timezone=True), nullable=True)
    ITEM_KEY = Column(Integer, nullable=False)
    STATUS = Column(String(512), nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_BY = Column(String(512), nullable=False, default=os.getlogin())


class TransformerFilePeriodKey(cn.Connections.base):
    __tablename__ = 'TRANSFORMER_FILE_PERIOD_KEY'
    FILE_PERIOD_ID = Column(Integer, primary_key=True)
    LOAD_ID = Column(Integer, nullable=False)
    EVENT_KEY = Column(Integer, nullable=True)
    FILE_NAME = Column(String(512), nullable=True)
    VENDOR_KEY = Column(Integer, nullable=True)
    RETAILER_KEY = Column(Integer, nullable=True)
    DATASTREAM_SUB = Column(String(512), nullable=True)
    REQUEST_DATE = Column(DateTime(timezone=True), nullable=True)
    PERIOD_KEY = Column(Integer, nullable=False)
    STATUS = Column(String(512), nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_BY = Column(String(512), nullable=False, default=os.getlogin())


class MetaAttributeGroup(cn.Connections.base):
    __tablename__ = 'META_ATTRIBUTEGROUP'
    ATTRIBUTE_GROUP_ID = Column(Integer, primary_key=True)
    ATTR_GROUP = Column(String(512), nullable=False)
    ATTR_NAME = Column(String(512), nullable=True)
    VENDOR_KEY = Column(Integer, nullable=True)
    RETAILER_KEY = Column(Integer, nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class MetaMeasureCore(cn.Connections.base):
    __tablename__ = 'META_MEASURES_CORE'
    RETAILER_NAME = Column(String(60), primary_key=True, nullable=False)
    MEASURE_NAME = Column(String(128), primary_key=True, nullable=False)
    IntegerERNAL_MEASURE_NAME = Column(String(128), nullable=True)
    LOCATION_TYPE = Column(String(10), nullable=False)
    UNITSORSALES = Column(String(1), nullable=False)
    PIT = Column(Boolean, nullable=False)
    REQUIRE_CASE_CONVERSION = Column(Boolean, nullable=False)
    REQUIRE_COUNT_MEASURE = Column(Boolean, nullable=False)
    REQUIRE_EQUI_CONVERSION = Column(Boolean, nullable=False)
    REQUIRE_AVG = Column(Boolean, nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    __table_args__ = (PrimaryKeyConstraint('RETAILER_NAME', 'MEASURE_NAME'),)


class MetaMeasureCoreAmountType(cn.Connections.base):
    __tablename__ = 'META_MEASURES_CORE_AMOUNTTYPE'
    RETAILER_NAME = Column(String(60), primary_key=True, nullable=False)
    MEASURE_NAME = Column(String(128), primary_key=True, nullable=False)
    AMOUNTTYPE_KEY = Column(String(128), primary_key=True, nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    __table_args__ = (PrimaryKeyConstraint('RETAILER_NAME', 'MEASURE_NAME', 'AMOUNTTYPE_KEY'),)


class MetaSubVendorMapping(cn.Connections.base):
    __tablename__ = 'META_SUBVENDOR_MAPPING'
    SUB_VENDOR_MAPPING_ID = Column(Integer, primary_key=True)
    RETAILER_KEY = Column(Integer, nullable=False)
    SUBVENDOR_KEY = Column(Integer, nullable=False)
    SUBVENDOR_ID = Column(String(255), nullable=False)
    SUBVENDOR_NAME = Column(String(255), nullable=False)
    SUBVENDOR_SNAME = Column(String(255), nullable=False)
    VENDOR_KEY = Column(Integer, nullable=False)
    VENDOR_NAME = Column(String(255), nullable=False)
    VENDOR_SNAME = Column(String(255), nullable=False)
    VENDOR_ACTIVE = Column(Boolean, nullable=False)
    SUBVENDOR_ACTIVE = Column(Boolean, nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class MetaTransformScript(cn.Connections.base):
    __tablename__ = 'META_TRANSFORM_SCRIPTS'
    TRANSFORM_SCRIPT_ID = Column(Integer, primary_key=True)
    TRANSFORMER = Column(String(512), nullable=False)
    DATASTREAM_SUB = Column(String(512), nullable=True)
    TRANSFORM_SCRIPT = Column(String(max), nullable=False)
    SCRIPT_TYPE = Column(String(255), nullable=False)
    EXECUTE_ORDER = Column(Integer, nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class DataStreamFactTableColumns(cn.Connections.base):
    __tablename__ = 'DATASTREAM_FACT_TABLE_COLUMNS'
    DATA_STREAM_FACT_ID = Column(Integer,primary_key=True)
    DATASTREAM_SUB = Column(String(512), nullable=False)
    COLUMN_NAME = Column(String(512), nullable=False)
    COLUMN_TYPE = Column(String(512), nullable=False)
    COLUMN_CONSTRAIN = Column(String(1024), nullable=False)
    COLUMN_POSITION = Column(Integer, nullable=False)
    UNPIVOT_TYPE = Column(String(512), nullable=False)
    IS_BUSINESS_KEY = Column(Boolean, nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class DataStreamUnPivotColumns(cn.Connections.base):
    __tablename__ = 'DATASTREAM_UNPIVOT_COLUMNS'
    DATA_STREAM_UNPIVOT_ID = Column(Integer, primary_key=True)
    DATASTREAM_SUB = Column(String(512), nullable=False)
    COLUMN_NAME = Column(String(max), nullable=False)
    AMOUNT_TYPE_KEY = Column(Integer, nullable=False)
    UNPIVOT_TYPE = Column(String(512), nullable=False)
    CURRENCY_TYPE_KEY = Column(Integer, nullable=False)
    NOT_LOAD_ZERO = Column(Boolean, nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class DataStreamFactTableConfig(cn.Connections.base):
    __tablename__ = 'DATASTREAM_FACT_TABLE_CONFIG'
    DATA_STREAM_CONFIG_ID = Column(Integer, primary_key=True)
    DATASTREAM_SUB = Column(String(512), nullable=False)
    IS_FACT = Column(Boolean, nullable=False)
    IS_TALL_TABLE = Column(Boolean, nullable=False)
    PRIMARY_KEY_COLUMN = Column(String(max), nullable=False)
    ORDER_BY_COLUMN = Column(String(max), nullable=False)
    SEGMENTED_CLAUSE = Column(String(max), nullable=False)
    PARTITION_CLAUSE = Column(String(max), nullable=False)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class TablesToPurge(cn.Connections.base):
    __tablename__ = 'TABLES_TO_PURGE'
    PURGE_ID = Column(Integer,primary_key=True)
    TABLE_NAME = Column(String(512), nullable=False)
    PARTITION_COLUMN = Column(String(512), nullable=False)
    MAX_DAYS_TO_KEEP = Column(Integer, nullable=False)
    PART_FUNCTION = Column(String(max), nullable=False)
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())


class DataStreamAvailableFiles(cn.Connections.base):
    __tablename__ = 'DATASTREAM_AVAILABLE_FILES'
    ID = Column(Integer, primary_key=True, nullable=False)
    FILE_NAME = Column(String(512), nullable=False)
    FILE_EXT = Column(String(512), nullable=False)
    FILE_SIZE = Column(Integer, nullable=False)
    FILE_ABS_PATH = Column(String(4096), nullable=False)
    MODIFIED_DATE = Column(DateTime(timezone=True), nullable=False)
    CONSUMED = Column(DateTime(timezone=True), nullable=False)
    CONSUMED_DATE = Column(DateTime(timezone=True), nullable=True)
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    PROCESS_PRIORITY = Column(Integer, nullable=False)


class RSICoreConfigProperty(cn.Connections.base):
    __tablename__ = 'RSI_CORE_CFGPROPERTY'
    SILO_ID = Column(String(100), primary_key=True)
    Name = Column(String(128), primary_key=True)
    Scope = Column(String(128), nullable=True)
    Category = Column(String(128), nullable=True)
    Value = Column(String, nullable=True)
    Description = Column(String(256), nullable=True)
    Type = Column(Integer, nullable=True)
    Transient = Column(Integer, nullable=True)
    Editable = Column(Integer, nullable=True)
    RestartRequired = Column(Integer, nullable=True)
    Encrypted = Column(Integer, nullable=True)
    Hidden = Column(Integer, nullable=True)
    ValidateDesc = Column(String(256), nullable=True)
    ValidateType = Column(String(128), nullable=True)
    __table_args__ = (PrimaryKeyConstraint('SILO_ID', 'Name'),)


class MetaScripts(cn.Connections.base):
    __tablename__ = 'META_SCRIPTS'
    SCRIPT_NAME = Column(String(1024), primary_key=True)
    CLASS_NAME = Column(String(255), primary_key=True)
    METHOD_NAME = Column(String(255), primary_key=True)
    DESCRIPTION = Column(String(2048), nullable=True)
    SCRIPT = Column(String(1024), nullable=True)
    SCRIPT_TYPE = Column(String(64), nullable=True)
    USAGE = Column(String, nullable=True)
    CREATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    CREATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATE_DATE = Column(DateTime(timezone=True), nullable=False, default=func.now())
    UPDATED_BY = Column(String(512), nullable=False, default=os.getlogin())
    __table_args__ = (PrimaryKeyConstraint('SCRIPT_NAME', 'CLASS_NAME', 'METHOD_NAME'),)


class RSIVariable(cn.Connections.base):
    __tablename__ = 'RSI_VARIABLES'
    VARIABLE_NAME = Column(String(50),primary_key=True, nullable=False)
    VARIABLE_VALUE = Column(String(100), nullable=True)