def enum(**enums):
    return type('Enum', (), enums)


ProcessStatus = enum(PROCESSING='PROCESSING', PROCESSED='PROCESSED', ERROR='ERROR')