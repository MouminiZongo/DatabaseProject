''' Insert data into an sqlite database and query it. '''

import glob
import json
import os.path
import sqlite3
from neo4j import GraphDatabase, basic_auth
import neo4j.exceptions
import pprint
from bson.code import Code
from pymongo import MongoClient


dataDir = 'data/'
jsonDir = os.path.join(dataDir, 'json')
dbFile = os.path.join(dataDir, 'sqlite.db')

# change this to False to avoid repopulating the databases each time
loadData = True
doSQLite = True
doNeo4j = True
doMongo = True


def main():
    if doSQLite:
        connection = sqlite3.connect(dbFile)
        cursor = connection.cursor()
        if loadData:
            createSchema(cursor)
            populateSqlite(jsonDir, cursor)
        querySqlite(cursor)
        connection.commit()
        connection.close()
    if doNeo4j:
        if loadData:
            populateNeo4j(jsonDir, True)
        queryNeo4j()
    if doMongo:
        if loadData:
            populateMongo(jsonDir)
        queryMongo()


################################################################################
#                                                                              #
#                          SQLite Setup/Aux Functions                          #
#                                                                              #
################################################################################

# you don't need to do anything here

def createSchema(cursor, clearDb=True):
    ''' Create necessary tables in the sqlite database '''

    # drop all tables if requested
    if clearDb:
        cursor.execute('DROP TABLE IF EXISTS landmark_located_at_location;')
        cursor.execute('DROP TABLE IF EXISTS image_tagged_web_entity;')
        cursor.execute('DROP TABLE IF EXISTS image_contains_landmark;')
        cursor.execute('DROP TABLE IF EXISTS image_matches_image;')
        cursor.execute('DROP TABLE IF EXISTS image_in_page;')
        cursor.execute('DROP TABLE IF EXISTS image_tagged_label;')
        cursor.execute('DROP TABLE IF EXISTS web_entity;')
        cursor.execute('DROP TABLE IF EXISTS location;')
        cursor.execute('DROP TABLE IF EXISTS landmark;')
        cursor.execute('DROP TABLE IF EXISTS page;')
        cursor.execute('DROP TABLE IF EXISTS label;')
        cursor.execute('DROP TABLE IF EXISTS image;')

    # Create image table
    create_0 = '''
        CREATE TABLE image (
            id          INTEGER PRIMARY KEY,
            url         VARCHAR(256),
            is_document INTEGER(1) DEFAULT 0
        );
    '''
    cursor.execute(create_0)

    # Create label table
    create_1 = '''
        CREATE TABLE label (
            id          INTEGER PRIMARY KEY,
            mid         VARCHAR(16),
            description VARCHAR(64)
        );
    '''
    cursor.execute(create_1)

    # Create image_tagged_label table
    create_2 = '''
        CREATE TABLE image_tagged_label (
            id       INTEGER PRIMARY KEY,
            image_id INTEGER,
            label_id INTEGER,
            score    REAL,
            FOREIGN KEY (image_id) REFERENCES image (id),
            FOREIGN KEY (label_id) REFERENCES label (id)
        );
    '''
    cursor.execute(create_2)

    # Create page table
    create_3 = '''
        CREATE TABLE page (
            id  INTEGER PRIMARY KEY,
            url VARCHAR(256)
        );
    '''
    cursor.execute(create_3)

    # Create landmark table
    create_4 = '''
        CREATE TABLE landmark (
            id          INTEGER PRIMARY KEY,
            mid         VARCHAR(16),
            description VARCHAR(64)
        );
    '''
    cursor.execute(create_4)

    # Create location table
    create_5 = '''
        CREATE TABLE location (
            id        INTEGER PRIMARY KEY,
            latitude  REAL,
            longitude REAL
        );
    '''
    cursor.execute(create_5)

    # Create web_entity table
    create_6 = '''
        CREATE TABLE web_entity (
            id          INTEGER PRIMARY KEY,
            entity_id   VARCHAR(16),
            description VARCHAR(64)
        );
    '''
    cursor.execute(create_6)

    # Create image_in_page table
    create_7 = '''
        CREATE TABLE image_in_page (
            id       INTEGER PRIMARY KEY,
            image_id INTEGER,
            page_id  INTEGER,
            FOREIGN KEY (image_id) REFERENCES image (id),
            FOREIGN KEY (page_id) REFERENCES page (id)
        );
    '''
    cursor.execute(create_7)

    # Create image_matches_image table
    create_8 = '''
        CREATE TABLE image_matches_image (
            id        INTEGER PRIMARY KEY,
            image_id1 INTEGER,
            image_id2 INTEGER,
            type      VARCHAR(8),
            FOREIGN KEY (image_id1) REFERENCES image (id),
            FOREIGN KEY (image_id2) REFERENCES image (id)
        );
    '''
    cursor.execute(create_8)

    # Create image_contains_landmark table
    create_9 = '''
        CREATE TABLE image_contains_landmark (
            id          INTEGER PRIMARY KEY,
            image_id    INTEGER,
            landmark_id INTEGER,
            score       REAL,
            FOREIGN KEY (image_id) REFERENCES image (id),
            FOREIGN KEY (landmark_id) REFERENCES landmark (id)
        );
    '''
    cursor.execute(create_9)

    # Create image_tagged_web_entity table
    create_10 = '''
        CREATE TABLE image_tagged_web_entity (
            id            INTEGER PRIMARY KEY,
            image_id      INTEGER,
            web_entity_id INTEGER,
            score         REAL,
            FOREIGN KEY (image_id) REFERENCES image (id),
            FOREIGN KEY (web_entity_id) REFERENCES web_entity (id)
        );
    '''
    cursor.execute(create_10)

    # Create landmark_located_at_location table
    create_11 = '''
        CREATE TABLE landmark_located_at_location (
            id          INTEGER PRIMARY KEY,
            landmark_id INTEGER,
            location_id INTEGER,
            FOREIGN KEY (landmark_id) REFERENCES landmark (id),
            FOREIGN KEY (location_id) REFERENCES location (id)
        );
    '''
    cursor.execute(create_11)


def populateSqlite(jsonDir, cursor):
    ''' Load Google JSON results into sqlite (schema must be created before) '''

    cnt = 0
    # Find and process all json files in the directory
    for jsonFile in glob.glob(os.path.join(jsonDir, '*.json')):
        print('\n\nLoading', jsonFile, 'into sqlite')
        with open(jsonFile) as jf:
            jsonData = json.load(jf)
            insertImage(cursor, jsonData)
            cnt += 1

    print('\nLoaded', cnt, 'JSON documents into Sqlite\n')


def insertImage(cursor, jsonData):
    imageId = getOrCreateRow(cursor, 'image', 
        {'url': jsonData['url'], 'is_document': 1})
    print('Inserting Image With ID', imageId)

    # process labelAnnotations field
    for ann in jsonData['response']['labelAnnotations']:
        labelId = getOrCreateRow(cursor, 'label',
            {'mid': ann['mid'], 'description': ann['description']})
        ltiId = getOrCreateRow(cursor, 'image_tagged_label', 
            {'image_id': imageId, 'label_id': labelId, 'score': ann['score']})

    # process webDetection.fullMatchingImages field
    if 'fullMatchingImages' in jsonData['response']['webDetection']:
        for fmi in jsonData['response']['webDetection']['fullMatchingImages']:
            imageId2 = getOrCreateRow(cursor, 'image', {'url': fmi['url']})
            imiId = getOrCreateRow(cursor, 'image_matches_image', 
                {'image_id1': imageId, 'image_id2': imageId2, 'type': 'full'})
    
    # process webDetection.partialMatchingImages field
    if 'partialMatchingImages' in jsonData['response']['webDetection']:
        for pmi in \
        jsonData['response']['webDetection']['partialMatchingImages']:
            imageId2 = getOrCreateRow(cursor, 'image', {'url': pmi['url']})
            imiId = getOrCreateRow(cursor, 'image_matches_image', 
                {'image_id1': imageId, 'image_id2': imageId2, 
                 'type': 'partial'})
    
    # process webDetection.pagesWithMatchingImages field
    if 'pagesWithMatchingImages' in jsonData['response']['webDetection']:
        for pmi in \
        jsonData['response']['webDetection']['pagesWithMatchingImages']:
            pageId = getOrCreateRow(cursor, 'page', {'url': pmi['url']})
            iipId = getOrCreateRow(cursor, 'image_in_page', 
                {'image_id': imageId, 'page_id': pageId})
    
    # process webDetection.webEntities field 
    # (note: some webEntities have no description field)
    for ent in jsonData['response']['webDetection']['webEntities']:
        if 'description' in ent:
            desc = ent['description']
        else:
            desc = ''
        # shorter syntax: ent['description'] if 'description' in ent else ''
        webEntityId = getOrCreateRow(cursor, 'web_entity', 
            {'entity_id': ent['entityId'], 'description': desc})
        iteId = getOrCreateRow(cursor, 'image_tagged_web_entity', 
            {'image_id': imageId, 'web_entity_id': webEntityId, 
             'score': ent['score']})
    
    # process landmarkAnnotations and landmarkAnnotations.locations fields
    # (note: some landmarks have no description field)
    if 'landmarkAnnotations' in jsonData['response']:
        for lma in jsonData['response']['landmarkAnnotations']:
            if 'description' in lma:
                desc = lma['description']
            else:
                desc = ''
            landmarkId = getOrCreateRow(cursor, 'landmark', 
                {'mid': lma['mid'], 'description': desc})
            iclId = getOrCreateRow(cursor, 'image_contains_landmark', 
                {'image_id': imageId, 'landmark_id': landmarkId, 
                 'score': lma['score']})
            for loc in lma['locations']:
                locationId = getOrCreateRow(cursor, 'location', 
                    {'latitude': loc['latLng']['latitude'], 
                     'longitude': loc['latLng']['longitude']})
                lllId = getOrCreateRow(cursor, 'landmark_located_at_location', 
                    {'landmark_id': landmarkId, 'location_id': locationId})


def getOrCreateRow(cursor, table, dataDict):
    ''' Return the ID of a row of the given table with the given data.

    If the row does not already exists then create it first.  Existence is
    determined by matching on all supplied values.  Table is the table name,
    dataDict is a dict of {'attribute': value} pairs.
    '''

    whereClauses = ' AND '.join(['"{}" = :{}'.format(k, k) for k in dataDict])
    select = 'SELECT id FROM {} WHERE {}'.format(table, whereClauses)
    # print(select)

    cursor.execute(select, dataDict)
    res = cursor.fetchone()
    if res is not None:
        return res[0]

    fields = ','.join('"{}"'.format(k) for k in dataDict)
    values = ','.join(':{}'.format(k) for k in dataDict)
    insert = 'INSERT INTO {} ({}) values({})'.format(table, fields, values)
    # print(insert)
    cursor.execute(insert, dataDict)

    cursor.execute(select, dataDict)
    res = cursor.fetchone()
    if res is not None:
        return res[0]
    raise Exception('Something went wrong with ' + str(dataDict))


def querySqliteAndPrintResults(query, cursor, title='Running query:'):
    print()
    print(title)
    print(query)

    for record in cursor.execute(query):
        print(' ' * 4, end='')
        print('\t'.join([str(f) for f in record]))


################################################################################
#                                                                              #
#                          Neo4j Setup/Aux Functions                           #
#                                                                              #
################################################################################

# you don't need to do anything here

def populateNeo4j(jsonDir, clearDb=False):
    'Load the JSON results from google into neo4j'

    driver = GraphDatabase.driver(
        'bolt://localhost:7687', auth=basic_auth('neo4j', 'cisc4610'))
    session = driver.session()

    # From: https://stackoverflow.com/a/29715865/2037288
    deleteQuery = '''
    MATCH (n)
    OPTIONAL MATCH (n)-[r]-()
    WITH n,r LIMIT 50000
    DELETE n,r
    RETURN count(n) as deletedNodesCount
    '''

    # TODO: complete insert query to include all necessary entities and
    # relationships at once
    insertQuery = '''
    WITH $json as q
    MERGE (img:Image {url:q.url})
        ON CREATE SET img.isDocument = 'true'
        ON MATCH  SET img.isDocument = 'true'
    FOREACH (ann in q.response.labelAnnotations | 
        MERGE (lbl:Label {mid:ann.mid})
            ON CREATE SET lbl.description = ann.description
        MERGE (img)-[:TAGGED {score:ann.score}]->(lbl))
    FOREACH (fmi in q.response.webDetection.fullMatchingImages | 
        MERGE (img2:Image {url:fmi.url})
            ON CREATE SET img2.isDocument = 'false'
        MERGE (img)-[:MATCH {type:'full'}]->(img2))
    FOREACH (pma in q.response.webDetection.partialMatchingImages | 
        MERGE (img2:Image {url:pma.url})
            ON CREATE SET img2.isDocument = 'false'
        MERGE (img)-[:MATCH {type:'partial'}]->(img2))
    FOREACH (web in q.response.webDetection.webEntities | 
        MERGE (ent:WebEntity {entityId:web.entityId})
            ON CREATE SET ent.description = COALESCE(web.description, '')
        MERGE (img)-[:TAGGED {score:web.score}]->(ent))
    FOREACH (pmi in q.response.webDetection.pagesWithMatchingImages | 
        MERGE (pag:Page {url:pmi.url})
        MERGE (img)-[:IN]->(pag))
    FOREACH (lma in q.response.landmarkAnnotations | 
        MERGE (lan:Landmark {mid:lma.mid, 
                             description:COALESCE(lma.description, '')})
        MERGE (img)-[:CONTAINS {score:lma.score}]->(lan)
        FOREACH (loc in lma.locations |
            MERGE (lct:Location {latitude:loc.latLng.latitude, 
                                 longitude:loc.latLng.longitude})
            MERGE (lan)-[:LOCATED_AT]->(lct)))
    '''
    
    countQuery = '''
    MATCH (a) WITH DISTINCT LABELS(a) AS temp, COUNT(a) AS tempCnt
    UNWIND temp AS label
    RETURN label, SUM(tempCnt) AS cnt
    ORDER BY label
    '''

    if clearDb:
        result = session.run(deleteQuery)
        for record in result:
            print('Deleted', record['deletedNodesCount'], 'nodes')

    loaded = 0
    for jsonFile in glob.glob(os.path.join(jsonDir, '*.json')):
        print('Loading', jsonFile, 'into neo4j')
        with open(jsonFile) as jf:
            jsonData = json.load(jf)
            try:
                session.run(insertQuery, {'json': jsonData})
                loaded += 1
            except neo4j.exceptions.ClientError as ce:
                print(' ^^^^ Failed:', str(ce))

    print('\nLoaded', loaded, 'JSON documents into Neo4j\n')

    queryNeo4jAndPrintResults(countQuery, session, 'Neo4j now contains')

    session.close()


def queryNeo4jAndPrintResults(query, session, title='Running query:'):
    print()
    print(title)
    print(query)

    if not query.strip():
        return
    
    for record in session.run(query):
        print(' ' * 4, end='')
        for val in record:
            print(val, end='\t')
        print()


################################################################################
#                                                                              #
#                         MongoDB Setup/Aux Functions                          #
#                                                                              #
################################################################################

# you don't need to do anything here

def populateMongo(jsonDir, clearDb=True):
    'Load the JSON results from google into mongo'

    client = MongoClient()
    db = client.homework3
    collection = db.googleTagged

    if clearDb:
        client.homework3.googleTagged.delete_many({})

    for jsonFile in glob.glob(os.path.join(jsonDir, '*.json')):
        print('Loading', jsonFile, 'into mongo')
        with open(jsonFile) as jf:
            jsonData = json.load(jf)
        key = {'url': jsonData['url']}
        collection.update_one(key, {'$set': jsonData}, upsert=True);

    print('Mongo now contains', collection.count_documents({}), 'documents')


def aggregateMongoAndPrintResults(pipeline, collection, desc='Running query:'):
    print()
    print(desc)
    print('***************** Aggregate pipeline ****************')
    pprint.pprint(pipeline)
    print('********************** Results **********************')
    if len(pipeline) > 0:
        for result in collection.aggregate(pipeline):
            pprint.pprint(result)
    print('*****************************************************')


################################################################################
#                                                                              #
#                               Query functions                                #
#                                                                              #
################################################################################

# fill in SQL and Cypher queries as well as MongoDB aggregation pipelines below

def querySqlite(cursor):
    ''' Run necessary queries and print results '''

    # TODO: 1. List the 10 Images with the greatest number of Landmarks 
    # contained in them. List them in descending order of the number of 
    # Landmarks they contain, followed by their URL alphabetically. 
    # List only the Image URLs and the numbers of Landmarks.
    query_1 = '''
        SELECT img.url, COUNT(*)
        FROM   image img
        JOIN   image_contains_landmark icl
        ON     img.id == icl.image_id
        GROUP BY img.url
        ORDER BY COUNT(*) DESC, img.url
        LIMIT 10;
    '''
    querySqliteAndPrintResults(query_1, cursor, title='SQL Query 1')

    # TODO: 2. List all Landmark descriptions associated with more than one 
    # geographic Location in the data. List them in descending order of the 
    # number of Locations, followed by the description alphabetically.
    # List only the descriptions and the numbers of Locations.
    query_2 = '''
        SELECT lma.description, COUNT(*)
        FROM   landmark_located_at_location lll
        JOIN   landmark lma
        ON     lll.landmark_id == lma.id
        GROUP BY lma.description
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC, lma.description;
    '''
    querySqliteAndPrintResults(query_2, cursor, title='SQL Query 2')

    # TODO: 3. List the 10 Images with the greatest number of Image matches of 
    # either type (partial or full). List them in descending order of the number
    # of matches, followed by their URL alphabetically. 
    # List only the Image URLs and the numbers of matches.
    query_3 = '''
        SELECT img.url, COUNT(*)
        FROM   image img
        JOIN   image_matches_image imi
        ON     img.id == imi.image_id1
        OR     img.id == imi.image_id2
        GROUP BY img.id
        ORDER BY COUNT(*) DESC, img.url
        LIMIT 10;
    '''
    querySqliteAndPrintResults(query_3, cursor, title='SQL Query 3')

    # TODO: 4. List the 10 documents (Images for which there is a JSON file) 
    # with the largest number of relationships of any kind (with Labels, Pages, 
    # etc.). List them in descending order of the number of relationships, 
    # followed by their URL alphabetically. List only the Image URLs and the 
    # numbers of relationships.
    query_4 = '''
        WITH counts (image_id, cnt) AS (
            SELECT image_id, COUNT(*)
            FROM   image_tagged_label
            GROUP BY image_id
            
            UNION ALL
            
            SELECT image_id, COUNT(*) 
            FROM   image_in_page
            GROUP BY image_id
            
            UNION ALL
            
            SELECT image_id1, COUNT(*) 
            FROM   image_matches_image
            GROUP BY image_id1
            
            UNION ALL
            
            SELECT image_id2, COUNT(*) 
            FROM   image_matches_image
            WHERE  image_id1 != image_id2
            GROUP BY image_id2
            
            UNION ALL
            
            SELECT image_id, COUNT(*) 
            FROM   image_contains_landmark
            GROUP BY image_id
            
            UNION ALL
            
            SELECT image_id, COUNT(*) 
            FROM   image_tagged_web_entity
            GROUP BY image_id
        )
        SELECT image.url, SUM(counts.cnt) rel_cnt
        FROM   image
        JOIN   counts 
        ON     image.id == counts.image_id
        WHERE  image.is_document == 1
        GROUP BY image.url
        ORDER BY rel_cnt DESC, image.url
        LIMIT 10;
    '''
    querySqliteAndPrintResults(query_4, cursor, title='SQL Query 4')


def queryNeo4j():
    driver = GraphDatabase.driver(
        'bolt://localhost:7687', auth=basic_auth('neo4j', 'cisc4610'))
    session = driver.session()

    # TODO: 5. List the 10 Images with the greatest number of Landmarks 
    # contained in them. List them in descending order of the number of 
    # Landmarks they contain, followed by their URL alphabetically. 
    # List only the Image URLs and the numbers of Landmarks.
    # (same as SQL query 1)
    query_5 = '''
        MATCH (i:Image)-[:CONTAINS]->(lm:Landmark)
        RETURN i.url, COUNT(lm)
        ORDER BY COUNT(lm) DESC, i.url
        LIMIT 10;
    '''
    queryNeo4jAndPrintResults(query_5, session, title='Neo4j Query 1')

    # TODO: 6. List all Landmark descriptions associated with more than one 
    # geographic Location in the data. List them in descending order of the 
    # number of Locations, followed by the description alphabetically.
    # List only the descriptions and the numbers of Locations.
    # (same as SQL query 2)
    query_6 = '''
        MATCH (la:Landmark) -[:LOCATED_AT]->(lo:Location)
        WITH  la.description AS description, count(lo) AS cnt
        WHERE cnt > 1
        RETURN description, cnt
        ORDER BY cnt DESC, description;
    '''
    queryNeo4jAndPrintResults(query_6, session, title='Neo4j Query 2')

    # TODO: 7. List the 10 Images with the greatest number of Image matches of 
    # either type (partial or full). List them in descending order of the number
    # of matches, followed by their URL alphabetically. 
    # List only the Image URLs and the numbers of matches.
    # (same as SQL query 3)
    query_7 = '''
        MATCH (m:Image)-[:MATCH]-(n:Image)
        RETURN m.url, COUNT(n)
        ORDER BY COUNT(n) DESC, m.url
        LIMIT 10;
    '''
    queryNeo4jAndPrintResults(query_7, session, title='Neo4j Query 3')

    # TODO: 8. List the 10 documents (Images for which there is a JSON file) 
    # with the largest number of relationships of any kind (with Labels, Pages, 
    # etc.). List them in descending order of the number of relationships, 
    # followed by their URL alphabetically. List only the Image URLs and the 
    # numbers of relationships.
    # (same as SQL query 4)
    query_8 = '''
        MATCH (i:Image {isDocument:'true'})-[r]-()
        RETURN i.url, COUNT(r)
        ORDER BY COUNT(r) DESC, i.url
        LIMIT 10;
    '''
    queryNeo4jAndPrintResults(query_8, session, title='Neo4j Query 4')
    
    # TODO: 9. List all "Landmark" nodes associated with more than one 
    # geographic Location in the data. List them in descending order of the 
    # number of Locations, followed by their description alphabetically.
    # List only the description and the number of locations.
    # (slightly different from SQL query 2 and Neo4j query 2)
    # TODO: In a comment below, briefly explain why this query returns fewer 
    # results than the one above. What about the data causes this?
    query_9 = '''
        MATCH (la:Landmark) -[:LOCATED_AT]->(lo:Location)
        WITH  la, COUNT(lo) AS cnt
        WHERE cnt > 1
        RETURN la.description, cnt
        ORDER BY cnt DESC, la.description;
    '''
    # the landmark descriptions "Statue of Liberty" and "New York City" each 
    # appear in the data with two different mid's and two different locations;
    # this causes two nodes to be created for each of these descriptions; 
    # individually, these nodes only have one location each, but when grouped by
    # description, they have two; that's why they are included in the results of
    # query 6 but not those of query 9
    queryNeo4jAndPrintResults(query_9, session, title='Neo4j Query 5')


    session.close()


def queryMongo():
    client = MongoClient()
    db = client.homework3
    collection = db.googleTagged

    # TODO: 10. List the 10 Images with the greatest number of Landmarks 
    # contained in them. List them in descending order of the number of 
    # Landmarks they contain, followed by their URL alphabetically. 
    # List only the Image URLs and the numbers of Landmarks.
    # (same as SQL query 1 and Neo4j query 1)
    pipeline_1 = [
        {'$project': {'cnt': {'$size': {'$ifNull': [
                                            '$response.landmarkAnnotations',
                                            []]}},
                      'url': 1,
                      '_id': 0}},
        {'$sort': {'cnt': -1, 'url': 1}},
        {'$limit': 10}
    ]
    aggregateMongoAndPrintResults(pipeline_1, collection, 'MongoDB pipeline 1')

    # TODO: 11. List all Landmark descriptions associated with more than one 
    # geographic Location in the data. List them in descending order of the 
    # number of Locations, followed by the description alphabetically.
    # List only the descriptions and the numbers of Locations.
    # (same as SQL query 2 and Neo4j query 2)
    pipeline_2 = [
        {'$unwind': {'path': '$response.landmarkAnnotations'}},
        {'$unwind': {'path': '$response.landmarkAnnotations.locations'}},
        {'$group': {'_id': {'landmark': 
                                '$response.landmarkAnnotations.description',
                            'location': 
                                '$response.landmarkAnnotations.locations.latLng'
                           }}},
        {'$group': {'_id': '$_id.landmark', 'cnt': {'$sum': 1}}},
        {'$sort': {'cnt': -1, '_id': 1}},
        {'$match': {'$expr': {'$gt': ['$cnt', 1]}}}
    ]
    aggregateMongoAndPrintResults(pipeline_2, collection, 'MongoDB pipeline 2')

    # TODO: 12. List the 10 Images with the greatest number of Landmarks 
    # contained in them. List them in descending order of the number of 
    # Landmarks they contain, followed by their URL alphabetically. 
    # List only the Image URLs and the numbers of Landmarks.
    # (same as SQL query 3 and Neo4j query 3)
    pipeline_3 = [
        {'$set': {'allMatches': {'$concatArrays': [
            {'$ifNull': ['$response.webDetection.partialMatchingImages', []]},
            {'$ifNull': ['$response.webDetection.fullMatchingImages', []]}
            ]}}},
        {'$addFields': {'matchCount': {'$size': '$allMatches'}}},
        {'$sort': {'matchCount': -1, 'url': 1}},
        {'$limit': 10},
        {'$project': {'_id': 0, 'matchCount': '$matchCount', 'url': '$url'}}
    ]
    aggregateMongoAndPrintResults(pipeline_3, collection, 'MongoDB pipeline 3')

    # TODO: 13. List the 10 documents (Images for which there is a JSON file) 
    # with the largest number of relationships of any kind (with Labels, Pages, 
    # etc.). List them in descending order of the number of relationships, 
    # followed by their URL alphabetically. List only the Image URLs and the 
    # numbers of relationships.
    # (same as SQL query 4 and Neo4j query 4)
    pipeline_4 = [
        {'$set': {
            'cnt_entities': {'$size': {'$ifNull': 
                ['$response.webDetection.webEntities', []]}},
            'cnt_full_matches': {'$size': {'$ifNull': 
                ['$response.webDetection.fullMatchingImages', []]}},
            'cnt_labels': {'$size': {'$ifNull': 
                ['$response.labelAnnotations', []]}},
            'cnt_landmarks': {'$size': {'$ifNull': 
                ['$response.landmarkAnnotations', []]}},
            'cnt_pages': {'$size': {'$ifNull': 
                ['$response.webDetection.pagesWithMatchingImages', []]}},
            'cnt_partial_matches': {'$size': {'$ifNull': 
                ['$response.webDetection.partialMatchingImages', []]}}}},
        {'$project': {'_id': 0,
                      'cnt': {'$sum': ['$cnt_labels',
                                       '$cnt_landmarks',
                                       '$cnt_pages',
                                       '$cnt_entities',
                                       '$cnt_partial_matches',
                                       '$cnt_full_matches']},
                      'url': 1}},
        {'$sort': {'cnt': -1, 'url': 1}},
        {'$limit': 10}
    ]
    aggregateMongoAndPrintResults(pipeline_4, collection, 'MongoDB pipeline 4')


if __name__ == '__main__':
    main()
