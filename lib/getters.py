#!/usr/bin/env python3
# coding: utf8

# ==============================================================================
# Title: MEDOC
# Description: MEDOC launch
# Author: Emeric Dynomant
# Contact: emeric.dynomant@omictools.com
# Date: 11/08/2017
# Language release: python 3.5.2
# ==============================================================================


import sys
import pymysql.cursors
from lib.sql_helper import Query_Executor

managment_fields = ['filename', 'available_pmid', 'processed_pmid', 'available_pmid_list']

''' - - - - - - - - - - - - - -  
medline_citation
- - - - - - - - - - - - - -  '''



def get_medline_citation(insert_table):
    # Table fields list
    fields_medline_citation = ['date_completed', 'pub_date_day', 'citation_owner', 'iso_abbreviation', 'article_title',
                               'volume', 'vernacular_title', 'pub_date_year', 'date_revised',
                               'date_of_electronic_publication', 'article_author_list_comp_yn', 'medline_pgn',
                               'date_created', 'country', 'xml_file_name', 'medline_date', 'number_of_references',
                               'data_bank_list_comp_yn', 'nlm_unique_id', 'abstract_text', 'citation_status',
                               'grantlist_complete_yn', 'copyright_info', 'issue', 'journal_title', 'issn',
                               'pub_date_month', 'pmid', 'medline_ta']
    values_medline_citation = []
    citation = insert_table['value']

    values_medline_citation, fields_medline_citation = __extract_values_array__(fields_medline_citation, values_medline_citation, citation)

    return values_medline_citation, fields_medline_citation


def send_medline_citation(fields_medline_citation, values_tot_medline_citation, parameters):
    sql_command = 'INSERT INTO ' + 'medline_citation' + ' (' + ', '.join(
        fields_medline_citation) + ') VALUES ' + ', '.join(values_tot_medline_citation) + ' ;'
    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_article_language
- - - - - - - - - - - - - -  '''


def get_medline_article_language(insert_table):
    fields_medline_article_language = ['pmid', 'language']
    values_medline_article_language = []
    language = insert_table['value']

    values_medline_article_language, fields_medline_article_language = __extract_values_array__(fields_medline_article_language,
                                                                                values_medline_article_language, language)
    return values_medline_article_language, fields_medline_article_language


def send_medline_article_language(fields_medline_article_language, values_tot_medline_article_language, parameters):
    sql_command = 'INSERT INTO ' + 'medline_article_language' + ' (' + ', '.join(
        fields_medline_article_language) + ') VALUES ' + ', '.join(values_tot_medline_article_language) + ' ;'

    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_article_publication_type
- - - - - - - - - - - - - -  '''


def get_medline_article_publication_type(insert_table):
    fields_medline_article_publication_type = ['pmid', 'publication_type']
    values_medline_article_publication_type = []
    publication_type = insert_table['value']

    values_medline_article_publication_type, fields_medline_article_publication_type = __extract_values_array__(
        fields_medline_article_publication_type,
        values_medline_article_publication_type, publication_type)

    return values_medline_article_publication_type, fields_medline_article_publication_type


def send_medline_article_publication_type(fields_medline_article_publication_type,
                                          values_tot_medline_article_publication_type, parameters):
    sql_command = 'INSERT INTO ' + 'medline_article_publication_type' + ' (' + ', '.join(
        fields_medline_article_publication_type) + ') VALUES ' + ', '.join(
        values_tot_medline_article_publication_type) + ' ;'
    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_author
- - - - - - - - - - - - - -  '''


def get_medline_author(insert_table):
    fields_medline_author = ['pmid', 'last_name', 'fore_name', 'first_name', 'middle_name', 'initials', 'suffix',
                             'affiliation', 'collective_name']
    values_medline_author = []
    author = insert_table['value']

    values_medline_author, fields_medline_author = __extract_values_array__(
        fields_medline_author, values_medline_author, author)
    return values_medline_author, fields_medline_author


def send_medline_author(fields_medline_author, values_tot_medline_author, parameters):
    sql_command = 'INSERT INTO ' + 'medline_author' + ' (' + ', '.join(fields_medline_author) + ') VALUES ' + ', '.join(
        values_tot_medline_author) + ' ;'
    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_chemical_list
- - - - - - - - - - - - - -  '''


def get_medline_chemical_list(insert_table):
    fields_medline_chemical_list = ['pmid', 'registry_number', 'name_of_substance']
    values_medline_chemical_list = []
    chemical = insert_table['value']

    values_medline_chemical_list, fields_medline_chemical_list = __extract_values_array__(
        fields_medline_chemical_list, values_medline_chemical_list, chemical)
    return values_medline_chemical_list, fields_medline_chemical_list


def send_medline_chemical_list(fields_medline_chemical_list, values_tot_medline_chemical_list, parameters):
    sql_command = 'INSERT INTO ' + 'medline_chemical_list' + ' (' + ', '.join(
        fields_medline_chemical_list) + ') VALUES ' + ', '.join(values_tot_medline_chemical_list) + ' ;'
    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_citation_other_id
- - - - - - - - - - - - - -  '''


def get_medline_citation_other_id(insert_table):
    fields_medline_citation_other_id = ['pmid', 'source', 'other_id']
    values_medline_citation_other_id = []
    citation_other_id = insert_table['value']

    values_medline_citation_other_id, fields_medline_citation_other_id = __extract_values_array__(
        fields_medline_citation_other_id, values_medline_citation_other_id, citation_other_id)
    return values_medline_citation_other_id, fields_medline_citation_other_id


def send_medline_citation_other_id(fields_medline_citation_other_id, values_tot_medline_citation_other_id, parameters):
    sql_command = 'INSERT INTO ' + 'medline_citation_other_id' + ' (' + ', '.join(
        fields_medline_citation_other_id) + ') VALUES ' + ', '.join(values_tot_medline_citation_other_id) + ' ;'
    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_citation_subsets
- - - - - - - - - - - - - -  '''


def get_medline_citation_subsets(insert_table):
    fields_medline_citation_subsets = ['pmid', 'citation_subset']
    values_medline_citation_subsets = []
    subset = insert_table['value']

    values_medline_citation_subsets, fields_medline_citation_subsets = __extract_values_array__(
        fields_medline_citation_subsets, values_medline_citation_subsets, subset
    )
    return values_medline_citation_subsets, fields_medline_citation_subsets


def send_medline_citation_subsets(fields_medline_citation_subsets, values_tot_medline_citation_subsets, parameters):
    sql_command = 'INSERT INTO ' + 'medline_citation_subsets' + ' (' + ', '.join(
        fields_medline_citation_subsets) + ') VALUES ' + ', '.join(values_tot_medline_citation_subsets) + ' ;'

    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_comments_corrections
- - - - - - - - - - - - - -  '''


def get_medline_comments_corrections(insert_table):
    fields_medline_comments_corrections = ['pmid', 'ref_pmid', 'type', 'ref_source']
    values_medline_comments_corrections = []
    comments_correction = insert_table['value']

    values_medline_comments_corrections, fields_medline_comments_corrections = __extract_values_array__(
        fields_medline_comments_corrections, values_medline_comments_corrections, comments_correction
    )
    return values_medline_comments_corrections, fields_medline_comments_corrections


def send_medline_comments_corrections(fields_medline_comments_corrections, values_tot_medline_comments_corrections,
                                      parameters):
    sql_command = 'INSERT INTO ' + 'medline_comments_corrections' + ' (' + ', '.join(
        fields_medline_comments_corrections) + ') VALUES ' + ', '.join(values_tot_medline_comments_corrections) + ' ;'
    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_data_bank
- - - - - - - - - - - - - -  '''


def get_medline_data_bank(insert_table):
    fields_medline_data_bank = ['pmid', 'accession_number']
    values_medline_data_bank = []
    data_bank = insert_table['value']

    values_medline_data_bank, fields_medline_data_bank = __extract_values_array__(
        fields_medline_data_bank, values_medline_data_bank, data_bank
    )
    return values_medline_data_bank, fields_medline_data_bank


def send_medline_data_bank(fields_medline_data_bank, values_tot_medline_data_bank, parameters):
    sql_command = 'INSERT INTO ' + 'medline_data_bank' + ' (' + ', '.join(
        fields_medline_data_bank) + ') VALUES ' + ', '.join(values_tot_medline_data_bank) + ' ;'
    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_grant
- - - - - - - - - - - - - -  '''


def get_medline_grant(insert_table):
    fields_medline_grant = ['pmid', 'grant_id', 'acronym', 'agency', 'country']
    values_medline_grant = []
    grant = insert_table['value']

    values_medline_grant, fields_medline_grant = __extract_values_array__(
        fields_medline_grant, values_medline_grant, grant
    )
    return values_medline_grant, fields_medline_grant


def send_medline_grant(fields_medline_grant, values_tot_medline_grant, parameters):
    sql_command = 'INSERT INTO ' + 'medline_grant' + ' (' + ', '.join(fields_medline_grant) + ') VALUES ' + ', '.join(
        values_tot_medline_grant) + ' ;'
    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_investigator
- - - - - - - - - - - - - -  '''


def get_medline_investigator(insert_table):
    fields_medline_investigator = ['pmid', 'last_name', 'fore_name', 'first_name', 'middle_name', 'initials', 'suffix',
                                   'affiliation']
    values_medline_investigator = []
    investigator = insert_table['value']

    values_medline_investigator, fields_medline_investigator = __extract_values_array__(
        fields_medline_investigator, values_medline_investigator, investigator
    )
    return values_medline_investigator, fields_medline_investigator


def send_medline_investigator(fields_medline_investigator, values_tot_medline_investigator, parameters):
    sql_command = 'INSERT INTO ' + 'medline_investigator' + ' (' + ', '.join(
        fields_medline_investigator) + ') VALUES ' + ', '.join(values_tot_medline_investigator) + ' ;'
    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_mesh_heading
- - - - - - - - - - - - - -  '''


def get_medline_mesh_heading(insert_table):
    fields_medline_mesh_heading = ['pmid', 'descriptor_name', 'descriptor_ui', 'descriptor_name_major_yn',
                                   'qualifier_name', 'qualifier_ui', 'qualifier_name_major_yn']
    values_medline_mesh_heading = []
    mesh_heading = insert_table['value']

    values_medline_mesh_heading, fields_medline_mesh_heading = __extract_values_array__(
        fields_medline_mesh_heading, values_medline_mesh_heading, mesh_heading
    )
    return values_medline_mesh_heading, fields_medline_mesh_heading


def send_medline_mesh_heading(fields_medline_mesh_heading, values_tot_medline_mesh_heading, parameters):
    sql_command = 'INSERT INTO ' + 'medline_mesh_heading' + ' (' + ', '.join(
        fields_medline_mesh_heading) + ') VALUES ' + ', '.join(values_tot_medline_mesh_heading) + ' ;'
    Query_Executor(parameters).execute(sql_command)


''' - - - - - - - - - - - - - -  
medline_personal_name_subject
- - - - - - - - - - - - - -  '''


def get_medline_personal_name_subject(insert_table):
    fields_medline_personal_name_subject = ['pmid', 'last_name', 'fore_name', 'first_name', 'middle_name', 'initials',
                                            'suffix']
    values_medline_personal_name_subject = []
    personal_name_subject = insert_table['value']

    values_medline_personal_name_subject, fields_medline_personal_name_subject = __extract_values_array__(
        fields_medline_personal_name_subject, values_medline_personal_name_subject, personal_name_subject
    )
    return values_medline_personal_name_subject, fields_medline_personal_name_subject


def send_medline_personal_name_subject(fields_medline_personal_name_subject, values_tot_medline_personal_name_subject,
                                       parameters):
    sql_command = 'INSERT INTO ' + 'medline_personal_name_subject' + ' (' + ', '.join(
        fields_medline_personal_name_subject) + ') VALUES ' + ', '.join(values_tot_medline_personal_name_subject) + ' ;'
    Query_Executor(parameters).execute(sql_command)


def send_management(field_list, value_list, parameters):
    # make sure the values are string
    value_list = ['"%s"' % str(value) for value in value_list]
    sql_command = 'INSERT INTO ' + 'medline_managment' + ' (' + ', '.join(
        field_list) + ') VALUES (' + ', '.join(value_list) + ') ;'
    Query_Executor(parameters).execute(sql_command)


def __extract_values_array__(fields_names, values_output, key_pair_object):
    """
    Given a list of keys, an empty array of values to fill and an object of keys-values, extract all the values 
    referred in the list of keys

    :param fields_names: 
    :param values_output: 
    :param key_pair_object: 
    :return: 
    """
    for field in fields_names:
        # By default we assume there is not value
        value_to_append = '"N/A"'
        if field in key_pair_object:
            value_to_append = __dequote_value__(key_pair_object, field)
            # Add it to a list
            values_output.append(value_to_append)

    return values_output, fields_names


def __dequote_value__(fields_dictionary, field):
    # Get "VALUE"
    value = fields_dictionary[field]
    element = value
    if type(value) == list:
        element = '"N/A"'
        # Get the first element
        if len(value) > 0:
            element = value[0]
        # TODO Although this seems to be happen only for qualifiers, it should be handled correctly.
        # For example it should be printed in the error log file or in a warning log file
        # if len(value) > 1:
        #     print('Warning: Discarding %d values for pmid %s and field %s and values %s' % (
        #         len(value) - 1, fields_dictionary['pmid'], field, ','.join(value)), file=sys.stderr
        #     )

    # Remove quotes and quote the result and make sure there is no escape sequence lead character at the end of the
    # string
    # TODO I should not clean data here, but use the escape functionality provided by the MySQL module
    if element.endswith('\\'):
        element = element[:-1]
    value_to_append = '"' + element.replace('"', '') + '"'
    return value_to_append
