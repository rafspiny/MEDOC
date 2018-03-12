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

import os
import re
import sys
import time
import configparser

sys.path.append('./lib')
import MEDOC
import getters

if __name__ == '__main__':

    MEDOC = MEDOC.MEDOC()
    parameters = configparser.ConfigParser()
    parameters.read('./configuration.cfg')
    insert_limit = int(parameters['database']['insert_command_limit'])
    insert_log_path = os.path.join(parameters['paths']['program_path'], parameters['paths']['already_downloaded_files'])

    # Step A : Create database if not exist
    MEDOC.create_pubmedDB()

    # Step B: get file list on NCBI
    gz_file_list = MEDOC.get_file_list()

    for file_to_download in gz_file_list:
    # for file_to_download in ['baseline/pubmed18n0572.xml.gz']:

        start_time = time.time()

        if file_to_download not in open(insert_log_path).read().splitlines():

            # Step C: download file if not already
            file_downloaded = MEDOC.download(file_name=file_to_download)
            # file_downloaded = 'pubmed18n0572.xml.gz'

            # Step D: extract file
            file_content = MEDOC.extract(file_name=file_downloaded)

            # Step E: Parse XML file to extract articles
            articles = MEDOC.parse(data=file_content)

            print('- ' * 30 + 'SQL INSERTION')

            #  Timestamp
            start_time_sql = time.time()

            #  Lists to create
            values_tot_medline_citation = []
            values_tot_medline_article_language = []
            values_tot_medline_article_publication_type = []
            values_tot_medline_author = []
            values_tot_medline_chemical_list = []
            values_tot_medline_citation_other_id = []
            values_tot_medline_citation_subsets = []
            values_tot_medline_comments_corrections = []
            values_tot_medline_data_bank = []
            values_tot_medline_grant = []
            values_tot_medline_investigator = []
            values_tot_medline_mesh_heading = []
            values_tot_medline_personal_name_subject = []

            articles_count = 0
            processed_pmid = 0

            # Step F: Create a dictionary with data to INSERT for every article
            for raw_article in articles:

                #  Loading
                articles_count += 1
                if articles_count % 10000 == 0:
                    print('{} articles inserted for file {}'.format(articles_count, file_to_download))

                article_cleaned = re.sub('\'', ' ', str(raw_article))
                article_INSERT_list = MEDOC.get_command(article=article_cleaned, gz=file_downloaded)

                # Step G: For every table in articles, loop to create global insert
                for insert_table in article_INSERT_list:

                    #  ____ 1: medline_citation
                    if insert_table['name'] == 'medline_citation':
                        values_medline_citation = getters.get_medline_citation(insert_table)
                        values_tot_medline_citation.append('(' + ', '.join(values_medline_citation[0]) + ')')
                        if (len(values_tot_medline_citation) == insert_limit) or (
                                    articles_count == len(articles)):
                            processed_pmid += len(values_tot_medline_citation)
                            getters.send_medline_citation(values_medline_citation[1], values_tot_medline_citation,
                                                          parameters)
                            values_tot_medline_citation = []

                    #  ____ 2: medline_article_language
                    if insert_table['name'] == 'medline_article_language':
                        values_medline_article_language = getters.get_medline_article_language(insert_table)
                        values_tot_medline_article_language.append(
                            '(' + ', '.join(values_medline_article_language[0]) + ')')
                        if (len(values_tot_medline_article_language) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_article_language(values_medline_article_language[1],
                                                                  values_tot_medline_article_language, parameters)
                            values_tot_medline_article_language = []

                    #  ____ 3: medline_article_publication_type
                    if insert_table['name'] == 'medline_article_publication_type':
                        values_medline_article_publication_type = getters.get_medline_article_publication_type(
                            insert_table)
                        values_tot_medline_article_publication_type.append(
                            '(' + ', '.join(values_medline_article_publication_type[0]) + ')')
                        if (len(values_tot_medline_article_publication_type) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_article_publication_type(values_medline_article_publication_type[1],
                                                                          values_tot_medline_article_publication_type,
                                                                          parameters)
                            values_tot_medline_article_publication_type = []

                    #  ____ 4: medline_author
                    if insert_table['name'] == 'medline_author':
                        values_medline_author = getters.get_medline_author(insert_table)
                        values_tot_medline_author.append('(' + ', '.join(values_medline_author[0]) + ')')
                        if (len(values_tot_medline_author) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_author(values_medline_author[1], values_tot_medline_author, parameters)
                            values_tot_medline_author = []

                    #  ____ 5: medline_chemical_list
                    if insert_table['name'] == 'medline_chemical_list':
                        values_medline_chemical_list = getters.get_medline_chemical_list(insert_table)
                        values_tot_medline_chemical_list.append('(' + ', '.join(values_medline_chemical_list[0]) + ')')
                        if (len(values_tot_medline_chemical_list) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_chemical_list(values_medline_chemical_list[1],
                                                               values_tot_medline_chemical_list, parameters)
                            values_tot_medline_chemical_list = []

                    #  ____ 6: medline_citation_other_id
                    if insert_table['name'] == 'medline_citation_other_id':
                        values_medline_citation_other_id = getters.get_medline_citation_other_id(insert_table)
                        values_tot_medline_citation_other_id.append(
                            '(' + ', '.join(values_medline_citation_other_id[0]) + ')')
                        if (len(values_tot_medline_citation_other_id) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_citation_other_id(values_medline_citation_other_id[1],
                                                                   values_tot_medline_citation_other_id, parameters)
                            values_tot_medline_citation_other_id = []

                    #  ____ 7: medline_citation_subsets
                    if insert_table['name'] == 'medline_citation_subsets':
                        values_medline_citation_subsets = getters.get_medline_citation_subsets(insert_table)
                        values_tot_medline_citation_subsets.append(
                            '(' + ', '.join(values_medline_citation_subsets[0]) + ')')
                        if (len(values_tot_medline_citation_subsets) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_citation_subsets(values_medline_citation_subsets[1],
                                                                  values_tot_medline_citation_subsets, parameters)
                            values_tot_medline_citation_subsets = []

                    #  ____ 8: medline_comments_corrections
                    if insert_table['name'] == 'medline_comments_corrections':
                        values_medline_comments_corrections = getters.get_medline_comments_corrections(insert_table)
                        values_tot_medline_comments_corrections.append(
                            '(' + ', '.join(values_medline_comments_corrections[0]) + ')')
                        if (len(values_tot_medline_comments_corrections) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_comments_corrections(values_medline_comments_corrections[1],
                                                                      values_tot_medline_comments_corrections,
                                                                      parameters)
                            values_tot_medline_comments_corrections = []

                    #  ____ 9: medline_data_bank
                    if insert_table['name'] == 'medline_data_bank':
                        values_medline_data_bank = getters.get_medline_data_bank(insert_table)
                        values_tot_medline_data_bank.append('(' + ', '.join(values_medline_data_bank[0]) + ')')
                        if (len(values_tot_medline_data_bank) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_data_bank(values_medline_data_bank[1], values_tot_medline_data_bank,
                                                           parameters)
                            values_tot_medline_data_bank = []

                    #  ____ 10: medline_grant
                    if insert_table['name'] == 'medline_grant':
                        values_medline_grant = getters.get_medline_grant(insert_table)
                        values_tot_medline_grant.append('(' + ', '.join(values_medline_grant[0]) + ')')
                        if (len(values_tot_medline_grant) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_grant(values_medline_grant[1], values_tot_medline_grant, parameters)
                            values_tot_medline_grant = []

                    #  ____ 11: medline_investigator
                    if insert_table['name'] == 'medline_investigator':
                        values_medline_investigator = getters.get_medline_investigator(insert_table)
                        values_tot_medline_investigator.append('(' + ', '.join(values_medline_investigator[0]) + ')')
                        if (len(values_tot_medline_investigator) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_investigator(values_medline_investigator[1],
                                                              values_tot_medline_investigator, parameters)
                            values_tot_medline_investigator = []

                    #  ____ 12: medline_mesh_heading
                    if insert_table['name'] == 'medline_mesh_heading':
                        values_medline_mesh_heading = getters.get_medline_mesh_heading(insert_table)
                        values_tot_medline_mesh_heading.append('(' + ', '.join(values_medline_mesh_heading[0]) + ')')
                        if (len(values_tot_medline_mesh_heading) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_mesh_heading(values_medline_mesh_heading[1],
                                                              values_tot_medline_mesh_heading, parameters)
                            values_tot_medline_mesh_heading = []

                    #  ____ 13: medline_personal_name_subject
                    if insert_table['name'] == 'medline_personal_name_subject':
                        values_medline_personal_name_subject = getters.get_medline_personal_name_subject(insert_table)
                        values_tot_medline_personal_name_subject.append(
                            '(' + ', '.join(values_medline_personal_name_subject[0]) + ')')
                        if (len(values_tot_medline_personal_name_subject) == insert_limit) or (articles_count == len(articles)):
                            getters.send_medline_personal_name_subject(values_medline_personal_name_subject[1],
                                                                       values_tot_medline_personal_name_subject,
                                                                       parameters)
                            values_tot_medline_personal_name_subject = []

            # Step H: Write the remaining entries
            if len(values_tot_medline_citation) > 0:
                processed_pmid += len(values_tot_medline_citation)
                getters.send_medline_citation(values_medline_citation[1], values_tot_medline_citation,
                                              parameters)
            if len(values_tot_medline_article_language) > 0:
                getters.send_medline_article_language(values_medline_article_language[1],
                                                  values_tot_medline_article_language, parameters)
            if len(values_tot_medline_article_publication_type) > 0:
                getters.send_medline_article_publication_type(values_medline_article_publication_type[1],
                                                          values_tot_medline_article_publication_type,
                                                          parameters)
            if len(values_tot_medline_author) > 0:
                getters.send_medline_author(values_medline_author[1], values_tot_medline_author, parameters)

            if len(values_tot_medline_chemical_list) > 0:
                getters.send_medline_chemical_list(values_medline_chemical_list[1],
                                                       values_tot_medline_chemical_list, parameters)
            if len(values_tot_medline_citation_other_id) > 0:
                getters.send_medline_citation_other_id(values_medline_citation_other_id[1],
                                                   values_tot_medline_citation_other_id, parameters)
            if len(values_tot_medline_citation_subsets) > 0:
                getters.send_medline_citation_subsets(values_medline_citation_subsets[1],
                                              values_tot_medline_citation_subsets, parameters)
            if len(values_tot_medline_comments_corrections) > 0:
                getters.send_medline_comments_corrections(values_medline_comments_corrections[1],
                                              values_tot_medline_comments_corrections,
                                              parameters)
            if len(values_tot_medline_data_bank) > 0:
                getters.send_medline_data_bank(values_medline_data_bank[1], values_tot_medline_data_bank,
                                           parameters)
            if len(values_tot_medline_grant) > 0:
                getters.send_medline_grant(values_medline_grant[1], values_tot_medline_grant, parameters)
            if len(values_tot_medline_investigator) > 0:
                getters.send_medline_investigator(values_medline_investigator[1],
                                              values_tot_medline_investigator, parameters)
            if len(values_tot_medline_mesh_heading) > 0:
                getters.send_medline_mesh_heading(values_medline_mesh_heading[1],
                                              values_tot_medline_mesh_heading, parameters)
            if len(values_tot_medline_personal_name_subject) > 0:
                getters.send_medline_personal_name_subject(values_medline_personal_name_subject[1],
                                                       values_tot_medline_personal_name_subject,
                                                       parameters)

            # Get the list of PMIDs that are present in the document
            written_pmids = re.findall('MedlineCitation[^\n]*?>\n<PMID Version="\d">(\d+)</PMID>', str(articles),
                                       re.IGNORECASE | re.DOTALL)
            # Write the stats about the process
            getters.send_management(getters.managment_fields,
                                    [file_downloaded, len(articles), processed_pmid, ','.join(written_pmids)],
                                    parameters
                                    )

            values_tot_medline_citation = []
            values_tot_medline_article_language = []
            values_tot_medline_article_publication_type = []
            values_tot_medline_author = []
            values_tot_medline_chemical_list = []
            values_tot_medline_citation_other_id = []
            values_tot_medline_citation_subsets = []
            values_tot_medline_comments_corrections = []
            values_tot_medline_data_bank = []
            values_tot_medline_grant = []
            values_tot_medline_investigator = []
            values_tot_medline_mesh_heading = []
            values_tot_medline_personal_name_subject = []
            # Step I: remove file and add file_name to a list to ignore this file next time
            print(
                'Elapsed time: {} sec for module: {}'.format(round(time.time() - start_time_sql, 2), 'insert'))
            MEDOC.remove(file_name=file_to_download)
            print(
                'Total time for file {}: {} min\n'.format(file_to_download, round((time.time() - start_time) / 60, 2)))

            #  Flush RAM
            del articles
            del values_tot_medline_citation
            del values_tot_medline_article_language
            del values_tot_medline_article_publication_type
            del values_tot_medline_author
            del values_tot_medline_chemical_list
            del values_tot_medline_citation_other_id
            del values_tot_medline_citation_subsets
            del values_tot_medline_comments_corrections
            del values_tot_medline_data_bank
            del values_tot_medline_grant
            del values_tot_medline_investigator
            del values_tot_medline_mesh_heading
            del values_tot_medline_personal_name_subject
