from neo4j.v1 import GraphDatabase, basic_auth
import os

def interact(S):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            return tx.run(S).data()
def buildDict(nodeList):
    D = {}
    for node in nodeList:
        D.update({node.id:node.properties['uid']})
    return D

def retrieveRelations(L):
    Rels = {}
    S = ''
    count = 0
    if len(L) <= 1:
        return Rels
    for uid in L:
        if count == 0:
            S += 'MATCH (u:USER) WHERE u.uid = "' + uid + '" '
            count += 1
            continue
        S += 'OR u.uid = "' + uid + '" '
        count += 1
    S += 'WITH COLLECT(u) AS us UNWIND us AS u1 UNWIND us AS u2 MATCH (u1)-[r]->(u2) RETURN COLLECT(r),us;'
    R = interact(S)
    D = buildDict(R[0]['us'])
    for rel in R[0]['COLLECT(r)']:
        user = D[rel.end]
        follower = D[rel.start]
        if user not in Rels:
            Rels.update({user:{'followers':[follower]}})
        else:
            Rels[user]['followers'].append(follower)
    return Rels

driver = GraphDatabase.driver(
    os.environ.get('NEO4J_URI'), auth=(
        os.environ.get('NEO4J_USER'),
        os.environ.get('NEO4J_PASSWORD')
    )
)

#Example:
L = ['519152261','2202077121','840500312','2560889150','2598410551']
R = retrieveRelations(L)
print( R)
#the result will be like: R = {'2671674622': {'followers': ['2579804991', '2625681694']}, '2662674287': {'followers': ['2579804991', '2625681694']}}
