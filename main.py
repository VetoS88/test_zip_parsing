import asyncio
import io
import multiprocessing
import os
import random
import shutil
import typing
import zipfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from uuid import uuid4
from xml.dom import minidom
from xml.etree import ElementTree

import aiofiles


class ArchiveCreator:
    """
    Class produces and save 50 zip archives with special structure xml documents.
    """

    def __init__(self):
        self.archive_path_name = 'archives'
        self.target_archive_count = 50
        self.level_max_count = 100
        self.objects_elements_max_count = 10

    def create_xml(self) -> str:
        """
        Create xml document by special conditions
        :return:
        """
        root = minidom.Document()

        xml = root.createElement('root')
        root.appendChild(xml)

        var_id = root.createElement('var')
        var_id.setAttribute('name', 'id')
        unique_id = f"var_{uuid4()}"
        var_id.setAttribute('value', unique_id)
        xml.appendChild(var_id)

        var_level = root.createElement('var')
        var_level.setAttribute('name', 'level')
        levels_count = str(random.randint(1, self.level_max_count))
        var_level.setAttribute('value', levels_count)
        xml.appendChild(var_level)

        objects_container = root.createElement('objects')
        objects_count = random.randint(1, self.objects_elements_max_count)
        xml.appendChild(objects_container)
        for _ in range(objects_count):
            object_element = root.createElement('object')
            unique_object_name = f"obj_{uuid4()}"
            object_element.setAttribute('name', unique_object_name)
            objects_container.appendChild(object_element)

        return root.toxml()

    async def create_archive(self, archive_name: str) -> None:
        """
        Create archive from xml data
        :param archive_name:
        :return:
        """
        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
            for idx in range(100):
                target_xml = self.create_xml()
                zip_file.writestr(f"{idx}.xml", target_xml)

        async with aiofiles.open(archive_name, mode='ab') as f:
            await f.write(zip_buffer.getvalue())

    def create_archives_count(self, archive_count: int, scope_number: int, archive_path: str) -> None:
        """
        Creates archives chunk.
        :param archive_count:
        :param scope_number:
        :param archive_path:
        :return:
        """
        # print(f"create_archives_count: {scope_number=} {archive_count=} {archive_path=}")
        loop = asyncio.get_event_loop()
        tasks = []
        for idx in range(archive_count):
            task = loop.create_task(self.create_archive(f"{archive_path}/{scope_number}_{idx}.zip"))
            tasks.append(task)
        loop.run_until_complete(asyncio.gather(*tasks))

    def create_archives(self) -> str:
        """
        Creates archive in parallel way.
        Evenly archives count run on all cores.
        :return:
        """
        shutil.rmtree(self.archive_path_name, ignore_errors=True)
        archive_path = Path(self.archive_path_name)
        archive_path.mkdir()

        processor_core_count = os.cpu_count()

        with ProcessPoolExecutor(max_workers=processor_core_count) as executor:
            for idx in range(processor_core_count):
                archive_count = self.target_archive_count // processor_core_count
                if not idx:
                    extended_archives_count = self.target_archive_count % processor_core_count
                    archive_count = archive_count + extended_archives_count
                executor.submit(self.create_archives_count, archive_count, idx, archive_path)
        return self.archive_path_name


class ArchiveParser:
    """
    Archive parser class. Parse target directory and creates csv files with statistic for xml files
    """

    def __init__(self, queue_for_collect_levels, queue_for_collect_objects):
        self.queue_for_collect_levels = queue_for_collect_levels
        self.queue_for_collect_objects = queue_for_collect_objects

    def parse_xml(self, document_raw: bytes) -> None:
        """
        Parse xml from bytes
        Create target tuple
        Put into common queue
        :param document_raw:
        :return:
        """
        document_string = document_raw.decode('utf-8')
        document = ElementTree.fromstring(document_string)
        vars = {}
        for var in document.findall('var'):
            vars[var.get('name')] = var.get('value')

        objects_names = []

        for obj in document.findall('objects')[0].findall('object'):
            objects_names.append(obj.get('name'))

        doc_id = vars['id']
        doc_level = vars['level']
        self.queue_for_collect_levels.put((doc_id, doc_level))
        for object_name in objects_names:
            self.queue_for_collect_objects.put_nowait((doc_id, object_name))

    async def parse_archive(self, archive_name: str) -> None:
        """
        Read archive
        :param archive_name:
        :return:
        """
        async with aiofiles.open(archive_name, mode='rb') as f:
            archive_bytes = await f.read()

        with zipfile.ZipFile(io.BytesIO(archive_bytes), "r", zipfile.ZIP_DEFLATED, False) as zip_file:
            files_in_zip = zip_file.namelist()
            for file_name in files_in_zip:
                self.parse_xml(zip_file.read(file_name))

    @classmethod
    def parse_archives(cls, queue_for_collect_levels, queue_for_collect_objects, archives_list: typing.List[str]):
        loop = asyncio.get_event_loop()
        tasks = []
        archive_parser = cls(queue_for_collect_levels, queue_for_collect_objects)
        for archive_name in archives_list:
            task = loop.create_task(archive_parser.parse_archive(archive_name))
            tasks.append(task)
        loop.run_until_complete(asyncio.gather(*tasks))

    @classmethod
    def create_stats(cls, archive_path_name: str) -> str:
        """
        Run read source data in parallel
        :param archive_path_name:
        :return:
        """
        csv_path_name = 'archives_objects_statistic'
        mananger = multiprocessing.Manager()
        queue_for_collect_levels = mananger.Queue()
        queue_for_collect_objects = mananger.Queue()
        shutil.rmtree(csv_path_name, ignore_errors=True)
        archive_path = Path(csv_path_name)
        archive_path.mkdir()
        archive_files_names_list = os.listdir(archive_path_name)
        archive_files_list = [
            f"{archive_path_name}/{archive_file_name}" for archive_file_name in archive_files_names_list
        ]
        archives_count = len(archive_files_list)

        processor_core_count = os.cpu_count()
        print(f"Run on {processor_core_count} cores")

        start_index = 0
        processes = []
        for idx in range(processor_core_count):
            archive_count = (archives_count // processor_core_count) + start_index
            if not idx:
                extended_archives_count = archives_count % processor_core_count
                archive_count = archive_count + extended_archives_count
            archives_chunk = archive_files_list[start_index: archive_count]
            start_index = archive_count

            parce_process = multiprocessing.Process(
                target=cls.parse_archives,
                args=(queue_for_collect_levels, queue_for_collect_objects, archives_chunk),
            )
            parce_process.start()
            print(f"Run parallel process {parce_process}")
            processes.append(parce_process)

        for process in processes:
            process.join()

        levels_items = []
        while not queue_for_collect_levels.empty():
            item = queue_for_collect_levels.get()
            levels_items.append(item)

        with open(f"{csv_path_name}/levels.csv", "w") as levels_csv:
            for level_item in levels_items:
                level_line = ";".join(level_item) + "\n"
                levels_csv.write(level_line)

        objects_items = []
        while not queue_for_collect_objects.empty():
            item = queue_for_collect_objects.get()
            objects_items.append(item)

        with open(f"{csv_path_name}/objects.csv", "w") as levels_csv:
            for object_item in objects_items:
                object_line = ";".join(object_item) + "\n"
                levels_csv.write(object_line)
        return csv_path_name


class TestData:
    """
    Class for testing data.
    Read source file folder
    Read target files folder
    Compare data tuples
    """
    def __init__(self, archive_folder: str, csv_path_name: str) -> None:
        self.archive_folder = archive_folder
        self.csv_path_name = csv_path_name

    def parse_xml(self, document_raw: bytes):
        document_string = document_raw.decode('utf-8')
        document = ElementTree.fromstring(document_string)
        vars = {}
        for var in document.findall('var'):
            vars[var.get('name')] = var.get('value')

        objects_names = []

        for obj in document.findall('objects')[0].findall('object'):
            objects_names.append(obj.get('name'))

        doc_id = vars['id']
        doc_level = vars['level']

        doc_id_objects_names_pairs = []
        for object_name in objects_names:
            doc_id_objects_names_pairs.append((doc_id, object_name))

        return {
            "doc_level_pair": (doc_id, doc_level),
            "doc_id_objects_names_pairs": doc_id_objects_names_pairs
        }

    def test_parse_data(self) -> None:
        target_files = os.listdir(self.archive_folder)
        source_doc_level_pair = []
        source_doc_id_objects_names_pairs = []
        for target_file in target_files:
            target_archive = f"{self.archive_folder}/{target_file}"
            with zipfile.ZipFile(target_archive, "r", zipfile.ZIP_DEFLATED, False) as zip_file:
                files_in_zip = zip_file.namelist()
                for file_name in files_in_zip:
                    target_data = self.parse_xml(zip_file.read(file_name))
                    source_doc_level_pair.append(target_data["doc_level_pair"])
                    for object_pair in target_data["doc_id_objects_names_pairs"]:
                        source_doc_id_objects_names_pairs.append(object_pair)

        target_doc_level_pair = []
        target_doc_id_objects_names_pairs = []
        with open(f"{self.csv_path_name}/levels.csv") as levels_csv:
            for level_row in levels_csv:
                level_item = tuple(level_row.strip("\n").split(";"))
                target_doc_level_pair.append(level_item)

        with open(f"{self.csv_path_name}/objects.csv") as objects_csv:
            for object_row in objects_csv:
                object_item = tuple(object_row.strip("\n").split(";"))
                target_doc_id_objects_names_pairs.append(object_item)

        print("id level pairs equal", set(source_doc_level_pair) == set(target_doc_level_pair))
        print("id objects pairs equal",
              set(source_doc_id_objects_names_pairs) == set(target_doc_id_objects_names_pairs))


if __name__ == "__main__":
    print("==== Create archives ====")
    archive_folder = ArchiveCreator().create_archives()
    print("==== Parse archives ====")
    csv_path_name = ArchiveParser.create_stats(archive_folder)
    print("==== Test data ====")
    TestData(archive_folder, csv_path_name).test_parse_data()
