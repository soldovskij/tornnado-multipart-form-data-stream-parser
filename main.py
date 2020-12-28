import os
import hashlib
import uuid
from pathlib import Path


class MultiPartStreamerException(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class MultiPartStreamer:
    class MyBuffer:
        def __init__(self, part_name):
            self.buffer = b''
            self.part_name = part_name

        def write(self, chunk):
            self.buffer += chunk

        def close(self):
            pass

        def value(self):
            return self.buffer.decode()

        def __str__(self):
            return f'MyBuffer {self.part_name} {self.buffer}'

    class MyFile:
        def __init__(self, upload_dir, part_name, part_filename, part_filename_ext):
            self.file = self._create_file(upload_dir)
            self.part_filename_ext = part_filename_ext
            self.form_file_name = part_filename
            self.from_field_name = part_name
            self.dirname = upload_dir
            self.filename = None
            self.md5hash = None
            self.md5 = hashlib.md5()

        def _create_file(self, upload_dir):
            while True:
                log_id = str(uuid.uuid4())
                new_file_name = f'{log_id}.{self.part_filename_ext}'
                if not os.path.exists(new_file_name):
                    break

            return open(upload_dir / new_file_name, 'wb')

        def write(self, chunk):
            self.md5.update(chunk)
            self.file.write(chunk)

        def close(self):
            self.file.close()
            self.md5hash = self.md5.hexdigest()
            self.filename = f'{self.md5hash}.{self.part_filename_ext}'

            file = Path(self.file.name)
            file.rename(Path(self.dirname) / f'{self.filename}')

            self.file = None
            self.md5 = None

        def value(self):
            return self.filename

        def __str__(self):
            return f'MyFile {self.file.name}'

    def __init__(self, allowed_file_content_types: list, allowed_file_extensions: list, upload_dir: Path):
        self.boundary = None
        self.prev_chunk = b''
        self.parts = {}
        self.current_part_name = None
        self.mode = 0
        self.default_content_type = b'text/plain'
        self.upload_dir = upload_dir
        self.allowed_file_content_types = allowed_file_content_types
        self.allowed_file_extensions = allowed_file_extensions

    def _head_parser(self, head):
        splitted = head.split(b'\r\n')
        content_disposition = splitted[0]
        if len(splitted) > 1:
            content_type = splitted[1]
        else:
            content_type = b''

        return content_disposition, content_type

    def _content_type_parser(self, content_type):
        value = b''

        key = b'Content-Type: '
        key_len = len(key)

        start = content_type.find(key)
        if start != -1:
            value = content_type[key_len:]

        return value.decode()

    def _content_disposition_parser(self, content_disposition, field):
        # Content-Disposition: form-data; name="csrfmiddlewaretoken"
        value = b''

        key = str.encode(field) + b'="'
        key_len = len(key)

        start = content_disposition.find(key)
        if start != -1:
            tail = content_disposition[start + key_len:]
            end = tail.find(b'"')
            value = tail[:end]

        return value.decode()

    def _process_chunk_from_start(self, next_chunk):
        boundary = str.encode(f'--{self.boundary}\r\n')
        boundary_last = str.encode(f'\r\n--{self.boundary}--\r\n')

        double_chunk = self.prev_chunk + next_chunk
        splitted = double_chunk.split(boundary)[1:]

        for idx, s in enumerate(splitted):
            index = s.find(b'\r\n\r\n')
            head = s[:index]
            tail = s[index + 4:]

            content_disposition, content_type = self._head_parser(head)
            part_name = self._content_disposition_parser(content_disposition, 'name')
            part_type = self._content_type_parser(content_type) or self.default_content_type

            if not part_name:
                raise MultiPartStreamerException('Form field name required')

            self.current_part_name = part_name

            if part_type in self.allowed_file_content_types:
                part_filename = self._content_disposition_parser(content_disposition, 'filename')

                if not part_filename:
                    raise MultiPartStreamerException('Form filename required')

                if '.' not in part_filename:
                    raise MultiPartStreamerException('File extension required but not present')

                part_filename_ext = part_filename.split('.')[-1]

                if self.allowed_file_extensions:
                    if part_filename_ext not in self.allowed_file_extensions:
                        raise MultiPartStreamerException('Allowed file extension: ' + str(self.allowed_file_extensions))

                # Store file as new form part
                self.parts[part_name] = self.MyFile(self.upload_dir, part_name, part_filename, part_filename_ext)
            elif part_type == self.default_content_type:
                # Store buffer as new form part
                self.parts[part_name] = self.MyBuffer(part_name)
            else:
                raise MultiPartStreamerException('Allowed file content-type: ' + str(self.allowed_file_content_types))

            # when no brake abowe
            if tail[-len(boundary_last):] == boundary_last:
                # Finish current part and whole form
                self.parts[part_name].write(tail[:-len(boundary_last)])
                self.parts[part_name].close()
                self.prev_chunk = b''
            else:
                # When not last block in splitted
                if idx != len(splitted) - 1:
                    # Save part
                    self.parts[part_name].write(tail[:-len(b'\r\n')])
                else:
                    # Calculate with next chunk
                    self.prev_chunk = tail
                    self.mode = 1

    def _process_chunk_from_prev(self, next_chunk):
        boundary_mid = str.encode(f'\r\n--{self.boundary}\r\n')
        boundary_last = str.encode(f'\r\n--{self.boundary}--\r\n')

        # if boundary_last last was not found in mode 0 then continue find
        # boundary or boundary_last in mode 1
        part_name = self.current_part_name
        double_chunk = self.prev_chunk + next_chunk
        boundary_index = double_chunk.find(boundary_mid)

        if boundary_index != -1:
            # Case when start of boundary at end of prev_chunk and end of boundary at start of next_chunk
            # ----PREV_CHUNK-----
            # BBBBBBBBBBBB--BOUND
            # ----NEXT_CHUNK-----
            # ARY--BBBBBBBBBBBBBB
            # ----DOUBLE_CHUNK----
            # BBBBBBBBBBBB--BOUNDARY--BBBBBBBBBBBBBB
            # ^___save___^____preapre_to_mode_0____^

            # finish current part and start read next file
            self.parts[part_name].write(double_chunk[:boundary_index])
            self.parts[part_name].close()
            # boundary_index + is shift for process_chunk_from_start like as first chunk
            self.prev_chunk = double_chunk[boundary_index + len('\r\n'):]
            # unset file body loop
            self.mode = 0
        elif double_chunk[-len(boundary_last):] == boundary_last:
            # Case when whole boundary at end of double_chunk, also that mean end of form
            # ----double_chunk-----
            # BBBB--BOUNDARY_LAST--

            # finish current part and whole form
            self.parts[part_name].write(double_chunk[:-len(boundary_last)])
            self.parts[part_name].close()
            self.prev_chunk = b''
        else:
            # Case when boundary and boundary_last not found in part
            # ----double_chunk-----
            # BBBBBBBBBBBBBBBBBBBBB

            # part not finishd, save current and wait next call data_received
            self.parts[part_name].write(self.prev_chunk)
            self.prev_chunk = next_chunk

    def process(self, next_chunk):
        try:
            if self.mode == 0:
                self._process_chunk_from_start(next_chunk)

            elif self.mode == 1:
                self._process_chunk_from_prev(next_chunk)

        except MultiPartStreamerException:
            raise

        except Exception:
            try:
                self.parts[self.current_part_name].close()
            except Exception:
                raise MultiPartStreamerException(f'Can\'t close {self.current_part_name} on process exception occur')

    def values(self):
        # Do not call until last self.process was called, usualy it mean call in tornado.web.RequestHandler.post
        # Since unknow point to last self.process call must flush data before get values
        if self.prev_chunk:
            self._process_chunk_from_start(self.prev_chunk)
            self.prev_chunk = b''

        return {k: self.parts[k].value() for k in self.parts}

    def apply_content_type(self, content_type):
        try:
            v1, v2 = content_type.split(' ')

            from_format = v1.split(';')[0]
            from_boundary = v2.split('=')[1]

            if from_format and from_format == 'multipart/form-data':
                self.boundary = from_boundary
            else:
                raise ValueError

        except (ValueError, AttributeError):
            raise MultiPartStreamerException('Allow multipart/form-data only')
