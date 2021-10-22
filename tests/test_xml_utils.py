"""
Tests for the XML Utils functions
"""
from unittest import TestCase
import xmltodict
from lxml import etree

from src.xml_utils import (
    create_xml_library, format_experiment_xml, format_run_xml, format_submission_xml, get_title_prefix,
    make_read_length_list, xml_to_string)

SECRET = {'accession': 'phs001232', 'accession_version': 'v4'}


class TestXMLUtils(TestCase):
    """
    Tests for the XML Utils functions
    """

    def test_create_xml_library(self):
        """
        Test that the XML library is created correctly
        """
        xml_library = create_xml_library(
            'Blood', 'b2b0c9ad-1292-43cd-aeed-6b492e67252d', 'Illumina NovaSeq 5000',
            'd41d8cd98f00b204e9800998ecf8427e', '100, 100', 'GRCh37/hg19', 'e40d8f23-2f59-49b7-bb78-bf9fecc1beeb',
            SECRET, 2, 'b2b0c9ad-1292-43cd-aeed-6b492e67252d.bam')

        self.assertEqual(xml_library['attributes'], [['alignment_software', 'BWA v0.6.2']])
        self.assertEqual(xml_library['center'], 'HMS-CC')
        self.assertEqual(xml_library['design_description'], 'The exome capture utilizes the NimbleGen liquid capture on HGSC VCRome 2.1 that targets approximately 34 Mbp of genomic DNA including all coding exons of currently known disease genes (OMIM, HGMD, and GeneTests). To enhance the coverage of clinically relevant disease genes, the spike-in probe set (Exome 3 - PKV2) is used in 1:1.25 equimolar ratio with the VCRome exome capture design')
        self.assertEqual(xml_library['filename'], 'b2b0c9ad-1292-43cd-aeed-6b492e67252d')
        self.assertEqual(xml_library['instrument_model'], 'Illumina NovaSeq 5000')
        self.assertEqual(xml_library['library_layout'], 'PAIRED')
        self.assertEqual(xml_library['md5_checksum'], 'd41d8cd98f00b204e9800998ecf8427e')
        self.assertEqual(xml_library['phs_accession'], 'phs001232')
        self.assertEqual(xml_library['phs_accession_version'], 'v4')
        self.assertEqual(xml_library['platform'], 'ILLUMINA')
        self.assertEqual(xml_library['read_lengths'], [100, 100])
        self.assertEqual(xml_library['reference'], 'GRCh37/hg19')
        self.assertEqual(xml_library['sample_id'], 'e40d8f23-2f59-49b7-bb78-bf9fecc1beeb')
        self.assertEqual(xml_library['selection'], 'RANDOM')
        self.assertEqual(xml_library['source'], 'GENOMIC')
        self.assertEqual(xml_library['strategy'], 'WXS')
        self.assertEqual(
            xml_library['title'],
            'exome sequencing of homo sapiens: whole blood: Sample e40d8f23-2f59-49b7-bb78-bf9fecc1beeb')
        self.assertEqual(xml_library['upload_file_name'], 'b2b0c9ad-1292-43cd-aeed-6b492e67252d.bam')
        self.assertEqual(xml_library['latf_load'], False)

    def test_format_experiment_xml(self):
        """
        Tests that the experiment XML is created correctly
        """
        with open('./tests/mocks/experiment.xml') as experiment:
            experiment = xmltodict.parse(experiment.read())

        xml_library = create_xml_library(
            'Blood', 'b2b0c9ad-1292-43cd-aeed-6b492e67252d', 'Illumina NovaSeq 5000',
            'd41d8cd98f00b204e9800998ecf8427e', '100, 100', 'GRCh37/hg19', 'e40d8f23-2f59-49b7-bb78-bf9fecc1beeb',
            SECRET, 2, 'b2b0c9ad-1292-43cd-aeed-6b492e67252d.bam')

        result = xmltodict.parse(etree.tostring(format_experiment_xml(xml_library)))

        self.assertEqual(len(result['EXPERIMENT_SET']), len(experiment['EXPERIMENT_SET']))
        self.assertEqual(len(result['EXPERIMENT_SET']['EXPERIMENT']), len(experiment['EXPERIMENT_SET']['EXPERIMENT']))

        result_identifiers = result['EXPERIMENT_SET']['EXPERIMENT']['IDENTIFIERS']
        experiment_identifiers = experiment['EXPERIMENT_SET']['EXPERIMENT']['IDENTIFIERS']

        self.assertEqual(len(result_identifiers), len(experiment_identifiers))
        self.assertEqual(
            result_identifiers['SUBMITTER_ID']['@namespace'], experiment_identifiers['SUBMITTER_ID']['@namespace'])
        self.assertEqual(result_identifiers['SUBMITTER_ID']['#text'], experiment_identifiers['SUBMITTER_ID']['#text'])

        self.assertEqual(
            result['EXPERIMENT_SET']['EXPERIMENT']['TITLE'], experiment['EXPERIMENT_SET']['EXPERIMENT']['TITLE'])

        self.assertEqual(
            result['EXPERIMENT_SET']['EXPERIMENT']['STUDY_REF']['@accession'],
            experiment['EXPERIMENT_SET']['EXPERIMENT']['STUDY_REF']['@accession'])

        result_design = result['EXPERIMENT_SET']['EXPERIMENT']['DESIGN']
        experiment_design = experiment['EXPERIMENT_SET']['EXPERIMENT']['DESIGN']

        self.assertEqual(len(result_design), len(experiment_design))
        self.assertEqual(result_design['DESIGN_DESCRIPTION'], experiment_design['DESIGN_DESCRIPTION'])
        self.assertEqual(
            result_design['SAMPLE_DESCRIPTOR']['@refname'], experiment_design['SAMPLE_DESCRIPTOR']['@refname'])
        self.assertEqual(
            result_design['SAMPLE_DESCRIPTOR']['@refcenter'], experiment_design['SAMPLE_DESCRIPTOR']['@refcenter'])

        result_design_library_descriptor = result['EXPERIMENT_SET']['EXPERIMENT']['DESIGN']['LIBRARY_DESCRIPTOR']
        experiment_design_library_descriptor = experiment['EXPERIMENT_SET']['EXPERIMENT']['DESIGN']['LIBRARY_DESCRIPTOR']

        self.assertEqual(len(result_design_library_descriptor), len(experiment_design_library_descriptor))
        self.assertEqual(
            result_design_library_descriptor['LIBRARY_NAME'], experiment_design_library_descriptor['LIBRARY_NAME'])
        self.assertEqual(
            result_design_library_descriptor['LIBRARY_STRATEGY'],
            experiment_design_library_descriptor['LIBRARY_STRATEGY'])
        self.assertEqual(
            result_design_library_descriptor['LIBRARY_SOURCE'], experiment_design_library_descriptor['LIBRARY_SOURCE'])
        self.assertEqual(
            result_design_library_descriptor['LIBRARY_SELECTION'],
            experiment_design_library_descriptor['LIBRARY_SELECTION'])
        self.assertEqual(
            result_design_library_descriptor['LIBRARY_LAYOUT'], experiment_design_library_descriptor['LIBRARY_LAYOUT'])

        result_design_spot_descriptor = result['EXPERIMENT_SET']['EXPERIMENT']['DESIGN']['SPOT_DESCRIPTOR']
        experiment_design_spot_descriptor = experiment['EXPERIMENT_SET']['EXPERIMENT']['DESIGN']['SPOT_DESCRIPTOR']

        self.assertEqual(
            result_design_spot_descriptor['SPOT_DECODE_SPEC']['SPOT_LENGTH'],
            experiment_design_spot_descriptor['SPOT_DECODE_SPEC']['SPOT_LENGTH'])

        self.assertEqual(
            result_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][0]['READ_INDEX'],
            experiment_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][0]['READ_INDEX'])
        self.assertEqual(
            result_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][0]['READ_CLASS'],
            experiment_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][0]['READ_CLASS'])
        self.assertEqual(
            result_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][0]['READ_TYPE'],
            experiment_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][0]['READ_TYPE'])
        self.assertEqual(
            result_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][0]['BASE_COORD'],
            experiment_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][0]['BASE_COORD'])

        self.assertEqual(
            result_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][1]['READ_INDEX'],
            experiment_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][1]['READ_INDEX'])
        self.assertEqual(
            result_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][1]['READ_CLASS'],
            experiment_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][1]['READ_CLASS'])
        self.assertEqual(
            result_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][1]['READ_TYPE'],
            experiment_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][1]['READ_TYPE'])
        self.assertEqual(
            result_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][1]['BASE_COORD'],
            experiment_design_spot_descriptor['SPOT_DECODE_SPEC']['READ_SPEC'][1]['BASE_COORD'])

        result_platform = result['EXPERIMENT_SET']['EXPERIMENT']['PLATFORM']
        experiment_platform = experiment['EXPERIMENT_SET']['EXPERIMENT']['PLATFORM']

        self.assertEqual(len(result_platform), len(experiment_platform))
        self.assertEqual(
            result_platform['ILLUMINA']['INSTRUMENT_MODEL'], experiment_platform['ILLUMINA']['INSTRUMENT_MODEL'])

        result_experiment_attributes = result['EXPERIMENT_SET']['EXPERIMENT']['EXPERIMENT_ATTRIBUTES']
        experiment_experiment_attributes = experiment['EXPERIMENT_SET']['EXPERIMENT']['EXPERIMENT_ATTRIBUTES']

        self.assertEqual(len(result_experiment_attributes), len(experiment_experiment_attributes))
        self.assertEqual(
            len(result_experiment_attributes['EXPERIMENT_ATTRIBUTE']),
            len(experiment_experiment_attributes['EXPERIMENT_ATTRIBUTE']))
        self.assertEqual(
            result_experiment_attributes['EXPERIMENT_ATTRIBUTE']['TAG'],
            experiment_experiment_attributes['EXPERIMENT_ATTRIBUTE']['TAG'])
        self.assertEqual(
            result_experiment_attributes['EXPERIMENT_ATTRIBUTE']['VALUE'],
            experiment_experiment_attributes['EXPERIMENT_ATTRIBUTE']['VALUE'])

    def test_format_run_xml(self):
        """
        Tests that the run XML is created correctly
        """
        with open('./tests/mocks/run.xml') as run:
            run = xmltodict.parse(run.read())

        xml_library = create_xml_library(
            'Blood', 'b2b0c9ad-1292-43cd-aeed-6b492e67252d', 'Illumina NovaSeq 5000',
            'd41d8cd98f00b204e9800998ecf8427e', '100, 100', 'GRCh37/hg19', 'e40d8f23-2f59-49b7-bb78-bf9fecc1beeb',
            SECRET, 2, 'b2b0c9ad-1292-43cd-aeed-6b492e67252d.bam')

        result = xmltodict.parse(etree.tostring(format_run_xml(xml_library)))

        self.assertEqual(len(result['RUN_SET']), len(run['RUN_SET']))
        self.assertEqual(len(result['RUN_SET']['RUN']), len(run['RUN_SET']['RUN']))

        result_identifiers = result['RUN_SET']['RUN']['IDENTIFIERS']
        run_identifiers = run['RUN_SET']['RUN']['IDENTIFIERS']

        self.assertEqual(
            result_identifiers['SUBMITTER_ID']['@namespace'], run_identifiers['SUBMITTER_ID']['@namespace'])
        self.assertEqual(
            result_identifiers['SUBMITTER_ID']['#text'], run_identifiers['SUBMITTER_ID']['#text'])

        result_experiment_ref_identifiers = result['RUN_SET']['RUN']['EXPERIMENT_REF']['IDENTIFIERS']
        run_experiment_ref_identifiers = run['RUN_SET']['RUN']['EXPERIMENT_REF']['IDENTIFIERS']

        self.assertEqual(
            result_experiment_ref_identifiers['SUBMITTER_ID']['@namespace'],
            run_experiment_ref_identifiers['SUBMITTER_ID']['@namespace'])
        self.assertEqual(
            result_experiment_ref_identifiers['SUBMITTER_ID']['#text'],
            run_experiment_ref_identifiers['SUBMITTER_ID']['#text'])

        result_data_block = result['RUN_SET']['RUN']['DATA_BLOCK']
        run_data_block = run['RUN_SET']['RUN']['DATA_BLOCK']

        self.assertEqual(len(result_data_block['FILES']), len(run_data_block['FILES']))

        self.assertEqual(result_data_block['FILES']['FILE']['@checksum'], run_data_block['FILES']['FILE']['@checksum'])
        self.assertEqual(
            result_data_block['FILES']['FILE']['@checksum_method'], run_data_block['FILES']['FILE']['@checksum_method'])
        self.assertEqual(result_data_block['FILES']['FILE']['@filename'], run_data_block['FILES']['FILE']['@filename'])
        self.assertEqual(result_data_block['FILES']['FILE']['@filetype'], run_data_block['FILES']['FILE']['@filetype'])

        result_run_attributes = result['RUN_SET']['RUN']['RUN_ATTRIBUTES']
        run_run_attributes = run['RUN_SET']['RUN']['RUN_ATTRIBUTES']

        self.assertEqual(len(result_run_attributes), len(run_run_attributes))
        self.assertEqual(result_run_attributes['RUN_ATTRIBUTE']['TAG'], run_run_attributes['RUN_ATTRIBUTE']['TAG'])
        self.assertEqual(result_run_attributes['RUN_ATTRIBUTE']['VALUE'], run_run_attributes['RUN_ATTRIBUTE']['VALUE'])

    def test_format_submission_xml(self):
        """
        Tests that the submission XML is created correctly
        """
        with open('./tests/mocks/submission.xml') as submission:
            submission = xmltodict.parse(submission.read())

        xml_library = create_xml_library(
            'Blood', 'b2b0c9ad-1292-43cd-aeed-6b492e67252d', 'Illumina NovaSeq 5000',
            'd41d8cd98f00b204e9800998ecf8427e', '100, 100', 'GRCh37/hg19', 'e40d8f23-2f59-49b7-bb78-bf9fecc1beeb',
            SECRET, 2, 'b2b0c9ad-1292-43cd-aeed-6b492e67252d.bam')

        result = xmltodict.parse(etree.tostring(format_submission_xml(xml_library)))

        self.assertEqual(result['SUBMISSION']['@alias'], submission['SUBMISSION']['@alias'])
        self.assertEqual(result['SUBMISSION']['@center_name'], submission['SUBMISSION']['@center_name'])
        self.assertEqual(
            result['SUBMISSION']['@xsi:noNamespaceSchemaLocation'],
            submission['SUBMISSION']['@xsi:noNamespaceSchemaLocation'])
        self.assertEqual(result['SUBMISSION']['@xmlns:xsi'], submission['SUBMISSION']['@xmlns:xsi'])

        result_contacts = result['SUBMISSION']['CONTACTS']
        submission_contacts = submission['SUBMISSION']['CONTACTS']

        self.assertEqual(len(result_contacts), len(submission_contacts))
        self.assertEqual(result_contacts['CONTACT'][0]['@name'], submission_contacts['CONTACT'][0]['@name'])
        self.assertEqual(
            result_contacts['CONTACT'][0]['@inform_on_error'], submission_contacts['CONTACT'][0]['@inform_on_error'])
        self.assertEqual(
            result_contacts['CONTACT'][0]['@inform_on_status'], submission_contacts['CONTACT'][0]['@inform_on_status'])
        self.assertEqual(result_contacts['CONTACT'][1]['@name'], submission_contacts['CONTACT'][1]['@name'])
        self.assertEqual(
            result_contacts['CONTACT'][1]['@inform_on_error'], submission_contacts['CONTACT'][1]['@inform_on_error'])
        self.assertEqual(
            result_contacts['CONTACT'][1]['@inform_on_status'], submission_contacts['CONTACT'][1]['@inform_on_status'])

        result_actions = result['SUBMISSION']['ACTIONS']
        submission_actions = submission['SUBMISSION']['ACTIONS']

        self.assertEqual(len(result_actions), len(submission_actions))

        self.assertEqual(
            result_actions['ACTION'][0]['ADD']['@source'], submission_actions['ACTION'][0]['ADD']['@source'])
        self.assertEqual(
            result_actions['ACTION'][0]['ADD']['@schema'], submission_actions['ACTION'][0]['ADD']['@schema'])
        self.assertEqual(
            result_actions['ACTION'][1]['ADD']['@source'], submission_actions['ACTION'][1]['ADD']['@source'])
        self.assertEqual(
            result_actions['ACTION'][1]['ADD']['@schema'], submission_actions['ACTION'][1]['ADD']['@schema'])

    def test_get_title_prefix_exome(self):
        """
        Test that it creates the title prefix when sequencing type is exome
        """
        self.assertEqual(get_title_prefix(2, 'Blood'), 'exome sequencing of homo sapiens: whole blood: Sample ')

    def test_get_title_prefix_genome(self):
        """
        Test that it creates the title prefix when sequencing type is genome
        """
        self.assertEqual(get_title_prefix(3, 'Blood'), 'genome sequencing of homo sapiens: whole blood: Sample ')

    def test_get_title_prefix_transcriptome(self):
        """
        Test that it creates the title prefix when sequencing type is transcriptome
        """
        self.assertEqual(get_title_prefix(4, 'Blood'), 'RNA sequencing of homo sapiens: Blood: Sample ')

    def test_make_read_length_list(self):
        """
        Test that it converts the read lengths sting into an array of numbers
        """
        self.assertEqual(make_read_length_list('100, 100'), [100, 100])

    def test_xml_to_string(self):
        """
        Test that the XML is properly stringified, also tests indent
        """
        xml_root = etree.Element('root')
        xml_elem = etree.SubElement(xml_root, 'elem')
        xml_elem.text = 'value'

        xml = etree.ElementTree(xml_root)

        self.assertEqual(xml_to_string(xml), b'<root>\n  <elem>value</elem>\n</root>\n')
