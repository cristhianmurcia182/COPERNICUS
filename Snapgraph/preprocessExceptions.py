class PreprocessedCommandException(Exception):
    def __init__(self, message):
        super(PreprocessedCommandException, self).__init__(message)


class VHBandNotIncludedException(Exception):
    def __init__(self, message):
        super(VHBandNotIncludedException, self).__init__(message)


class VVBandNotIncludedException(Exception):
    def __init__(self, message):
        super(VVBandNotIncludedException, self).__init__(message)


class OrbitNotIncludedException(Exception):
    def __init__(self, message):
        super(OrbitNotIncludedException, self).__init__(message)


class ZipException(Exception):
    def __init__(self, message):
        super(ZipException, self).__init__(message)


class FilenameNotFoundException(Exception):
    def __init__(self, message):
        super(FilenameNotFoundException, self).__init__(message)
