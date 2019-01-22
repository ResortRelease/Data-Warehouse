CREATE INDEX ON :contact(email);
CREATE INDEX ON :contact(dealid);
CREATE INDEX ON :dispo(dispoid);

CREATE INDEX ON :marketing(id);

// Import the latest deal data
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///export-mk.csv" AS contact
MERGE (c:contact {dealid: toInteger(contact['dealid'])})
ON match set c.salesdate = contact['salesdate'], c.datecr = contact['datecr'], c.dateASAP = contact['dateASAP'], c.days = toInteger(contact['days']), c.status = contact['status'], c.email = contact['EmailAddress'], c.brand = contact['Brand'], c.source = contact['source'], c.sold = contact['was sold'], c.cancel = contact['cancelsale']

// Import the latest dispo
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///export-dispos.csv" AS dispo
MERGE (c:dispo {dispoid: toInteger(dispo['dispid'])})
ON match set c.date = dispo['date'], c.dealid = dispo['dealid'], c.userid = dispo['userid'], c.type = dispo['dispo']

// Create dispo relationship
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///export-dispos.csv" AS dispo
Match (c:contact {dealid: toInteger(dispo['dealid'])}), (d:dispo {dispoid: toInteger(dispo['dispid'])})
Merge (c)<-[:dispo_update]-(d)

// Import a marketing event
LOAD CSV WITH HEADERS FROM "file:///sms/sms-list.csv" AS message
Merge (a:marketing {  name: message['Name'] , type: "sms", date: message['Date'], version: message['Version'], image: message['Image'], text: message['Text'] })
WITH message
LOAD CSV WITH HEADERS FROM "file:///sms/" + message['Filename'] AS csv
Match (c:contact {dealid: csv['dealid']}), (m:marketing {name: message['Name']})
MERGE (c)-[:mrk_message]-(m)

// Find clients sent marketing message that closed.
Match (d:dispo)-[updates:dispo_update]-(c:contact)-[:mrk_message]-(m:marketing{date: "20190107"})
where c.salesdate > "2019-01-07" AND c.salesdate < "2019-01-09"
with c, m, collect(d) as dispos, count(d) as fullDispos
unwind dispos as dispo
match (dispo)-[]-(c)
where dispo.date > "01/07/2019"
return c.dealid, c.status, c.salesdate, c.datecr, c.dateASAP, c.days, m.name, m.version fullDispos, count(dispo) as after

// Import emails
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///emails.csv" AS email
MERGE (e:marketing {id: toInteger(email['id'])})
ON match set e.category_id = toInteger(email['category_id']), e.subject = email['subject'], e.name = email['name'], e.read_count = toInteger(email['read_count']), e.sent_count = toInteger(email['sent_count']), e.type = "email"

// Import email sent relationship
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///email-sent.csv" AS sent
Match (c:contact {email: sent['email_address']}), (m:marketing {id: toInteger(sent['email_id'])})
Merge (c)<-[s:mrk_message {date_sent: sent['date_sent'], is_read: sent['is_read'], date_read: sent['date_read'], source: sent['source'], open_count: toInteger(sent['open_count']), last_opened: sent['last_opened'], copy_id: sent['copy_id']}]-(m)