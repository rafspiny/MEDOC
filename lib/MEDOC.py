#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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
import time
import gzip
import configparser
from ftplib import FTP
import pymysql.cursors
from bs4 import BeautifulSoup
from lib.sql_helper import Query_Executor


class MEDOC(object):
    def __init__(self):
        """
		INIT
		"""
        self.parameters = self.config = configparser.ConfigParser()
        self.parameters.read('./configuration.cfg')
        self.regex_gz = re.compile('^pubmed.*.xml.gz$')
        self.insert_log_path = os.path.join(self.parameters['paths']['program_path'],
                                       self.parameters['paths']['already_downloaded_files'])
        self.download_folder = os.path.join(self.parameters['paths']['program_path'],
                                            self.parameters['paths']['pubmed_data_download'])
        print('\n' * 2)

    def create_pubmedDB(self):
        """
		DATABASE CREATION
		"""
        print('- ' * 30 + 'DATABASE CREATION')
        wished_schema_name = self.parameters['database']['database']
        #  Timestamp
        start_time = time.time()
        #  mySQL connexion
        connection = pymysql.connect(
            host=self.parameters.get('database', 'host'),
            port=self.parameters.getint('database', 'port'),
            user=self.parameters.get('database', 'user'),
            password=self.parameters.get('database', 'password'),
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
            )
        cursor = connection.cursor()
        #~ cursor.execute('')
        #  Check if 'pubmed' db exists, if not, create it by executing SQL file line by line
        cursor.execute('SHOW DATABASES ;')
        local_dbNames = []
        for row in cursor:
            local_dbNames.append(row['Database'])
        if wished_schema_name in local_dbNames:
            cursor.execute('USE %s ;' % wished_schema_name)
            cursor.execute('SHOW TABLES ;')
            print('Database %s already created with tables: ' % wished_schema_name)
            for row in cursor:
                print('\t- {}'.format(row['Tables_in_%s' % wished_schema_name]))
        else:
            print('Database %s doesn\'t exist. Creation ..' % wished_schema_name)
            cursor.execute('CREATE DATABASE %s ;' % wished_schema_name)
            cursor.execute('USE %s ;' % wished_schema_name)
            print('Sourcing file {}'.format(self.parameters['database']['path_to_sql']))
            for command in open(self.parameters['database']['path_to_sql'], 'r'):
                if command != '\n' and not command.startswith('#'):
                    cursor.execute(command)
            print('Database %s created with tables: ' % wished_schema_name)
            cursor.execute('SHOW TABLES ;')
            for row in cursor:
                print('\t- {}'.format(row['Tables_in_%s' % wished_schema_name]))

        print('Elapsed time: {} sec for module: {}'.format(round(time.time() - start_time, 2),
                                                           MEDOC.create_pubmedDB.__name__))
        cursor.close()
        connection.close()

    def get_imported_files(self):
        processed_files_query = """
        SELECT * 
        FROM medline_managment
        WHERE available_pmid = processed_pmid;"""
        results = Query_Executor(self.parameters).execute_select(processed_files_query)

        def normalize_management_data(data):
            """
            Explode the list of processed PubMed IDS
            :param data: 
            :return: 
            """
            pmid_list_key = 'available_pmid_list'
            if pmid_list_key in data.keys():
                data[pmid_list_key] = data[pmid_list_key].split(',')
            return data

        processed_files = [normalize_management_data(result) for result in results]
        processed_files = [element['filename'] for element in processed_files]

        return processed_files

    def get_file_list(self):
        print('- ' * 30 + 'EXTRACTING FILES LIST FROM FTP')
        #  Timestamp
        start_time = time.time()
        #  Create directory to keep file during INSERT
        if not os.path.exists(self.download_folder):
            os.makedirs(self.download_folder)
            inserted_log = open(self.insert_log_path, 'w')
            inserted_log.close()
        #  List of files to download
        gz_file_list = []
        #  Connect FTP's root
        ftp_ncbi = FTP('ftp.ncbi.nlm.nih.gov')
        ftp_ncbi.login()
        #  BASELINE
        gz_baseline = []
        ftp_ncbi.cwd('/pubmed/baseline/')
        file_list = ftp_ncbi.nlst()
        downloaded_files = os.listdir()
        for file_name in file_list:
            if re.match(self.regex_gz, file_name) is not None:
                gz_baseline.append('baseline/' + file_name)
        print('{} files in Medline\'s baseline'.format(len(gz_baseline)))
        #  UPDATES
        gz_update = []
        ftp_ncbi.cwd('/pubmed/updatefiles/')
        file_list = ftp_ncbi.nlst()
        downloaded_files = os.listdir()
        for file_name in file_list:
            if re.match(self.regex_gz, file_name) is not None:
                gz_update.append('updatefiles/' + file_name)
        print('{} files in Medline\'s updates'.format(len(gz_update)))
        #  If already INSERTED before
        inserted_log = open(self.insert_log_path, 'r')
        inserted_list = []
        for inserted_file_name in inserted_log:
            inserted_list.append(inserted_file_name)
        #  Join baseline + updates if not inserted already
        for baseline_file_name in gz_baseline:
            if baseline_file_name not in inserted_list:
                gz_file_list.append(baseline_file_name)
        for update_file_name in gz_update:
            if update_file_name not in inserted_list:
                gz_file_list.append(update_file_name)

        print('{} files to download not in your database'.format(len(gz_file_list) - len(inserted_list)))
        print('Elapsed time: {} sec for module: {}'.format(round(time.time() - start_time, 2),
                                                           MEDOC.get_file_list.__name__))

        return gz_file_list

    def download(self, file_name):
        print('- ' * 30 + 'DOWNLOADING FILE')
        #  Timestamp
        start_time = time.time()
        #  Go to storage directory
        os.chdir(self.download_folder)
        #  Connect FTP root
        ftp_ncbi = FTP('ftp.ncbi.nlm.nih.gov')
        ftp_ncbi.login()
        #  Change FTP directory
        file_name_dir = re.findall('(.*)/(.*)', file_name)
        ftp_ncbi.cwd('/pubmed/' + str(file_name_dir[0][0]))
        #  Download file
        file_handle = open(file_name_dir[0][1], 'wb')
        with file_handle:
            print('Downloading {} ..'.format(file_name))
            ftp_ncbi.retrbinary('RETR {}'.format(file_name_dir[0][1]), file_handle.write)
            os.chdir(self.parameters['paths']['program_path'])
            print('Elapsed time: {} sec for module: {}'.format(round(time.time() - start_time, 2),
                                                               MEDOC.download.__name__))

        return file_name_dir[0][1]

    def extract(self, file_name):
        print('- ' * 30 + 'FILE EXTRACTION')
        #  Timestamp
        start_time = time.time()
        os.chdir(self.download_folder)
        #  Extraction
        gz_file = gzip.open(file_name, 'rt', encoding='utf-8')
        file_content = gz_file.read()
        os.chdir(self.parameters['paths']['program_path'])
        print('Elapsed time: {} sec for module: {}'.format(round(time.time() - start_time, 2), MEDOC.extract.__name__))

        return file_content

    def parse(self, data):
        print('- ' * 30 + 'XML FILE PARSING')
        #  Timestamp
        start_time = time.time()
        #  Souping
        soup = BeautifulSoup(data, 'lxml')
        articles = soup.find_all('pubmedarticle')

        # TODO On huge file, BeautifulSoup will only return a very limited amount of tags for the fins_all.
        # Given that I am not able today to play with the BeautifulSoup parser and try to use a SAX one, I just double
        # check with regex if everything is ok, and in case not use regex to retrieve the data.
        # I experimented on a 270MB xml file from PubMed: pubmed18n0597.xml.gz
        matches = re.findall('<pubmedarticle>', data, re.IGNORECASE)
        if len(articles) != len(matches):
            print('The underlying parser failed to retrieve all the elements. Found {} out of {}. Loading the others '
                  'now...'.format(len(articles), len(matches)))
            # Something is not right
            articles = re.findall('(<pubmedarticle>.*?</pubmedarticle>)', data, re.IGNORECASE | re.DOTALL)
            articles = [article.lower() for article in articles]
            print('{} elements are now loaded.'.format(len(articles)))

        print('Elapsed time: {} sec for module: {}'.format(round(time.time() - start_time, 2), MEDOC.parse.__name__))
        del soup

        return articles

    def get_command(self, article, gz):

        soup_article = BeautifulSoup(str(article), 'lxml')
        article_INSERT_list = []
        pmid_primary_key = re.findall('<articleid idtype="pubmed">([0-9]*)</articleid>', str(article))

        #  Regexs
        r_year = re.compile('<year>([0-9]{4})</year>')
        r_month = re.compile('<month>([0-9]{2})</month>')
        r_day = re.compile('<day>([0-9]{2})</day>')

        ''' - - - - - - - - - - - - - -  
		medline_citation
		- - - - - - - - - - - - - -  '''
        #  date_created
        date_created = soup_article.find_all('datecreated')
        try:
            date_completed_value = [
                re.findall(r_year, str(date_created))[0] + '-' + re.findall(r_month, str(date_created))[0] + '-' +
                re.findall(r_day, str(date_created))[0]]
        except:
            date_completed_value = ['1900-01-01']
        #  date_completed
        date_completed = soup_article.find_all('datecompleted')
        try:
            date_completed_value = [
                re.findall(r_year, str(date_completed))[0] + '-' + re.findall(r_month, str(date_completed))[0] + '-' +
                re.findall(r_day, str(date_completed))[0]]
        except:
            date_completed_value = ['1900-01-01']
        #  date_revised
        date_revised = soup_article.find_all('daterevised')
        try:
            date_revised_value = [
                re.findall(r_year, str(date_revised))[0] + '-' + re.findall(r_month, str(date_revised))[0] + '-' +
                re.findall(r_day, str(date_revised))[0]]
        except:
            date_revised_value = ['1900-01-01']
        #  date_published
        date_published = soup_article.find_all('pubdate')
        #  journal
        journal = soup_article.find_all('journal')
        #  abstract_text_list
        abstract_text_list = re.findall('<abstracttext.*?>(.*?)</abstracttext>', str(article))
        if len(abstract_text_list) > 1:
            abstract_text_raw = ''.join(abstract_text_list)
            abstract_text = re.sub('\"', ' ', str(abstract_text_raw))
        else:
            abstract_text = abstract_text_list
        #  date_of_electronic_publication
        date_of_electronic_publication = soup_article.find_all('articledate', attrs={'datetype': 'Electronic'})
        try:
            date_of_electronic_publication_value = [re.findall(r_year, str(date_of_electronic_publication))[0] + '-' +
                                                    re.findall(r_month, str(date_of_electronic_publication))[0] + '-' +
                                                    re.findall(r_day, str(date_of_electronic_publication))[0]]
        except:
            date_of_electronic_publication_value = ['1900-01-01']
        #  MEDLINE infos
        medline_info_journal = soup_article.find_all('medlinejournalinfo')
        #  INSERT
        article_INSERT_list.append(
            {'name': 'medline_citation',
             'value': {
                 'pmid': pmid_primary_key,
                 'date_created': date_completed_value,
                 'date_completed': date_completed_value,
                 'date_revised': date_revised_value,
                 'issn': re.findall('<issn issntype=".*">(.*)</issn>', str(article)),
                 'volume': re.findall('<volume>([0-9]*)</volume>', str(article)),
                 'issue': re.findall('<issue>([0-9]*)</issue>', str(article)),
                 'pub_date_year': re.findall('<year>([0-9]{4})</year>', str(date_published)),
                 'pub_date_month': re.findall('<month>([0-9]{2}|\w+)</month>', str(date_published)),
                 'pub_date_day': re.findall('<day>([0-9]{2})</day>', str(date_published)),
                 'medline_date': re.findall('<medlinedate>(.*)</medlinedate>', str(date_published)),
                 'journal_title': re.findall('<title>(.*)</title>', str(journal)),
                 'iso_abbreviation': re.findall('<isoabbreviation>(.*)</isoabbreviation>', str(journal)),
                 'article_title': re.findall('<articletitle>(.*)</articletitle>', str(article)),
                 'medline_pgn': re.findall('<medlinepgn>(.*)</medlinepgn>', str(article)),
                 'abstract_text': abstract_text,
                 'copyright_info': re.findall('<copyrightinformation>(.*)</copyrightinformation>', str(article)),
                 'article_author_list_comp_yn': re.findall('<authorlist completeyn="([A-Z]{1})">', str(article)),
                 'data_bank_list_comp_yn': re.findall('<databanklist completeyn="([A-Z]{1})">', str(article)),
                 'grantlist_complete_yn': re.findall('<grantlist completeyn="([A-Z]{1})">', str(article)),
                 'vernacular_title': re.findall('<vernaculartitle>(.*)</vernaculartitle>', str(article)),
                 'date_of_electronic_publication': date_of_electronic_publication_value,
                 'country': re.findall('<country>(.*)</country>', str(medline_info_journal)),
                 'medline_ta': re.findall('<medlineta>(.*)</medlineta>', str(article)),
                 'nlm_unique_id': re.findall('<nlmuniqueid>(.*)</nlmuniqueid>', str(article)),
                 'xml_file_name': gz,
                 'number_of_references': re.findall('<numberofreferences>(.*)</numberofreferences>', str(article)),
                 'citation_owner': re.findall('<medlinecitation .*?owner="(.*?)".*?>', str(article)),
                 'citation_status': re.findall('<medlinecitation .*?status="([A-Za-z]*)".*?>', str(article))}
             }
        )

        ''' - - - - - - - - - - - - - -
		medline_article_language
		- - - - - - - - - - - - - -  '''
        languages_list = soup_article.find_all('language')
        for language in languages_list:
            article_INSERT_list.append(
                {'name': 'medline_article_language',
                 'value': {'pmid': pmid_primary_key,
                           'language': re.findall('<language>(.*)</language>', str(language))}
                 })

        ''' - - - - - - - - - - - - - -
		medline_article_publication_type
		- - - - - - - - - - - - - -  '''
        publication_type_list = soup_article.find_all('publicationtype')
        for publication_type in publication_type_list:
            article_INSERT_list.append(
                {'name': 'medline_article_publication_type',
                 'value': {'pmid': pmid_primary_key,
                           'publication_type': re.findall('<publicationtype ui=".*?">(.*?)</publicationtype>',
                                                          str(publication_type))}
                 })

        ''' - - - - - - - - - - - - - -
		medline_author
		- - - - - - - - - - - - - -  '''
        author_list = soup_article.find_all('author')
        for author in author_list:
            article_INSERT_list.append(
                {'name': 'medline_author',
                 'value': {'pmid': pmid_primary_key,
                           'last_name': re.findall('<lastname>(.*)</lastname>', str(author)),
                           'fore_name': re.findall('<forename>(.*)</forename>', str(author)),
                           'first_name': re.findall('<firstname>(.*)</firstname>', str(author)),
                           'middle_name': re.findall('<middlename>(.*)</middlename>', str(author)),
                           'initials': re.findall('<initials>(.*)</initials>', str(author)),
                           'suffix': re.findall('<suffix>(.*)</suffix>', str(author)),
                           'affiliation': re.findall('<affiliation>(.*)</affiliation>', str(author)),
                           'collective_name': re.findall('<collectivename>(.*)</collectivename>', str(author))}
                 })

        ''' - - - - - - - - - - - - - -  
		medline_chemical_list
		- - - - - - - - - - - - - -  '''
        chemical_list = soup_article.find_all('chemical')
        for chemical in chemical_list:
            article_INSERT_list.append(
                {'name': 'medline_chemical_list',
                 'value': {'pmid': pmid_primary_key,
                           'registry_number': re.findall('<registrynumber>(.*)</registrynumber>', str(chemical)),
                           'name_of_substance': re.findall('<nameofsubstance ui=".*">(.*)</nameofsubstance>',
                                                           str(chemical))}
                 })

        ''' - - - - - - - - - - - - - - 
		medline_citation_other_id
		- - - - - - - - - - - - - - '''
        other_ids_list = soup_article.find_all('otherid')
        for other_id in other_ids_list:
            article_INSERT_list.append(
                {'name': 'medline_citation_other_id',
                 'value': {'pmid': pmid_primary_key,
                           'source': re.findall('<otherid source="(.*)">.*</otherid>', str(other_id)),
                           'other_id': re.findall('<otherid source=".*">(.*)</otherid>', str(other_id))}
                 })

        ''' - - - - - - - - - - - - - - 
		medline_citation_subsets
		- - - - - - - - - - - - - - '''
        citation_subsets_list = soup_article.find_all('citationsubset')
        for citation_subsets in citation_subsets_list:
            article_INSERT_list.append(
                {'name': 'medline_citation_subsets',
                 'value': {'pmid': pmid_primary_key,
                           'citation_subset': re.findall('<citationsubset>(.*)</citationsubset>',
                                                         str(citation_subsets))}
                 })

        ''' - - - - - - - - - - - - - - 
		medline_comments_corrections
		- - - - - - - - - - - - - - '''
        medline_comments_corrections_list = soup_article.find_all('commentscorrections')
        for comment in medline_comments_corrections_list:
            article_INSERT_list.append(
                {'name': 'medline_comments_corrections',
                 'value': {'pmid': pmid_primary_key,
                           'ref_pmid': re.findall('<pmid version="1">(\d+)</pmid>', str(comment)),
                           'type': re.findall('<commentscorrections reftype="(.*?)">', str(comment)),
                           'ref_source': re.findall('<refsource>(.*)</refsource>', str(comment))}
                 })

        ''' - - - - - - - - - - - - - - 
		medline_data_bank
		- - - - - - - - - - - - - - '''
        medline_data_bank_list = soup_article.find_all('accessionnumber')
        for databank in medline_data_bank_list:
            article_INSERT_list.append(
                {'name': 'medline_data_bank',
                 'value': {'pmid': pmid_primary_key,
                           'accession_number': re.findall('<accessionnumber>(.*)</accessionnumber>', str(databank))}
                 })

        ''' - - - - - - - - - - - - - - 	
		medline_grant
		- - - - - - - - - - - - - - '''
        medline_grant_list = soup_article.find_all('grant')
        for grant in medline_grant_list:
            article_INSERT_list.append(
                {'name': 'medline_grant',
                 'value': {'pmid': pmid_primary_key,
                           'grant_id': re.findall('<grantid>(.*)</grantid>', str(grant)),
                           'acronym': re.findall('<acronym>(.*)</acronym>', str(grant)),
                           'agency': re.findall('<agency>(.*)</agency>', str(grant)),
                           'country': re.findall('<country>(.*)</country>', str(grant))}
                 })

        ''' - - - - - - - - - - - - - - 	
		medline_investigator
		- - - - - - - - - - - - - - '''
        medline_investigator_list = soup_article.find_all('investigator')
        for investigator in medline_investigator_list:
            article_INSERT_list.append(
                {'name': 'medline_investigator',
                 'value': {'pmid': pmid_primary_key,
                           'last_name': re.findall('<lastname>(.*)</lastname>', str(investigator)),
                           'fore_name': re.findall('<forename>(.*)</forename>', str(investigator)),
                           'first_name': re.findall('<firstname>(.*)</firstname>', str(investigator)),
                           'middle_name': re.findall('<middlename>(.*)</middlename>', str(investigator)),
                           'initials': re.findall('<initials>(.*)</initials>', str(investigator)),
                           'suffix': re.findall('<suffix>(.*)</suffix>', str(investigator)),
                           'affiliation': re.findall('<affiliation>(.*)</affiliation>', str(investigator))}
                 })

        ''' - - - - - - - - - - - - - - 
		medline_mesh_heading
		- - - - - - - - - - - - - - '''
        medline_mesh_heading_list = soup_article.find_all('meshheading')
        for mesh in medline_mesh_heading_list:
            article_INSERT_list.append(
                {'name': 'medline_mesh_heading',
                 'value': {'pmid': pmid_primary_key,
                           'descriptor_name': re.findall(
                               '<descriptorname .*majortopicyn="[A-Z]{1}".*?>(.*?)</descriptorname>', str(mesh)),

                           'descriptor_ui': re.findall(
                               '<descriptorname .*?ui="(D\d+)".*?>.*?</descriptorname>', str(mesh)),

                           'descriptor_name_major_yn': re.findall(
                               '<descriptorname .*?majortopicyn="([A-Z]{1})".*?>.*?</descriptorname>', str(mesh)),
                           'qualifier_name': re.findall(
                               '<qualifiername .*?>(.*?)</qualifiername>', str(mesh)),

                           'qualifier_ui': re.findall(
                               '<qualifiername .*?ui="(Q\d+)">.*?</qualifiername>',
                               str(mesh)),

                           'qualifier_name_major_yn': re.findall(
                               '<qualifiername .*?majortopicyn="([A-Z]{1})".*?>.*?</qualifiername>', str(mesh))
                           }
                 })

        ''' - - - - - - - - - - - - - - 
		medline_personal_name_subject
		- - - - - - - - - - - - - - '''
        medline_personal_name_subject_list = soup_article.find_all('personalnamesubject')
        for subject in medline_personal_name_subject_list:
            article_INSERT_list.append(
                {'name': 'medline_personal_name_subject',
                 'value': {'pmid': pmid_primary_key,
                           'last_name': re.findall('<lastname>(.*)</lastname>', str(subject)),
                           'fore_name': re.findall('<forename>(.*)</forename>', str(subject)),
                           'first_name': re.findall('<firstname>(.*)</firstname>', str(subject)),
                           'middle_name': re.findall('<middlename>(.*)</middlename>', str(subject)),
                           'initials': re.findall('<initials>(.*)</initials>', str(subject)),
                           'suffix': re.findall('<suffix>(.*)</suffix>', str(subject))}
                 })

        del soup_article
        #  Final return, for every articles
        return article_INSERT_list

    def remove(self, file_name):
        inserted_log = open(self.insert_log_path, 'a')
        inserted_log.write('{}\n'.format(file_name))
        inserted_log.close()
        os.chdir(self.download_folder)
        file_name = re.findall('(.*)/(.*)', file_name)[0][1]
        os.remove('./' + file_name)
