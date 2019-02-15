// 1) Set indexes
CREATE INDEX ON :contact(email);
CREATE CONSTRAINT ON (contact:contact) ASSERT contact.dealid IS UNIQUE
CREATE INDEX ON :dispo(type);

CREATE INDEX ON :marketing(id);
CREATE INDEX ON :marketing(name);

// 2) Import the latest deal
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///export-mk.csv" AS contact
MERGE (c:contact {dealid: toInteger(contact['dealid'])})
ON create set c.salesdate = contact['salesdate'], 
c.datecr = contact['datecr'], 
c.dateASAP = contact['dateASAP'], 
c.days = toInteger(contact['days']), 
c.status = contact['status'], 
c.email = contact['EmailAddress'], 
c.brand = contact['Brand'], 
c.source = contact['source'], 
c.sold = contact['was sold'], 
c.cancel = contact['cancelsale']

// 3) Import the latest dispo
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///dispos.csv" AS dispo
MERGE (c:dispo {type: dispo['dispo']})

// 4) Create dispo relationship
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///export-dispos.csv" AS dispo
Match (c:contact {dealid: toInteger(dispo['dealid'])}), (d:dispo {type: dispo['dispo']})
Merge (c)<-[du:dispo_update {dispoid: toInteger(dispo['dispid'])}]-(d)
ON create set du.date = dispo['date'], 
du.userid = toInteger(dispo['userid'])

// Import new only
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///export-dispos.csv" AS dispo
MERGE (c:contact {dealid: toInteger(dispo['dealid'])})-[du:dispo_update {dispoid: toInteger(dispo['dispid'])}]-(d:dispo {type: dispo['dispo']})
ON match set du.date = dispo['date'], 
du.userid = toInteger(dispo['userid'])

// 5) Import a marketing event
LOAD CSV WITH HEADERS FROM "file:///sms/sms-list.csv" AS message
Merge (a:marketing {
  name: message['Name'], 
  type: "sms", 
  date: message['Date'], 
  version: message['Version'], 
  image: message['Image'], 
  text: message['Text'] 
})
WITH message
LOAD CSV WITH HEADERS FROM "file:///sms/" + message['Filename'] AS csv
Match (c:contact {
  dealid: toInteger(csv['dealid'])
}), 
(m:marketing {name: message['Name']})
merge (c)<-[:mrk_message {date_sent: message['Date']}]-(m)

// 6) Import emails
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///emails.csv" AS email
MERGE (e:marketing {id: toInteger(email['id'])})
ON create set e.category_id = toInteger(email['category_id']), 
e.subject = email['subject'], 
e.name = email['name'], 
e.read_count = toInteger(email['read_count']), 
e.sent_count = toInteger(email['sent_count']), 
e.type = "email"

// 7) Create basic relationship
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///email-sent.csv" AS sent
Match (c:contact {
  email: sent['email_address']
}), (m:marketing {
  id: toInteger(sent['email_id'])
})
merge (c)<-[:mrk_message {id: toInteger(sent['id'])}]-(m)

// 8) Import email sent relationship
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///email-sent.csv" AS sent
Match (c:contact {email: sent['email_address']})-[mk:mrk_message{id: toInteger(sent['id'])}]-(m:marketing {id: toInteger(sent['email_id'])})
set mk.date_sent = sent['date_sent'], 
mk.is_read = sent['is_read'], 
mk.date_read = sent['date_read'], 
mk.source = sent['source'], 
mk.open_count = toInteger(sent['open_count']), 
mk.last_opened = sent['last_opened'], 
mk.copy_id = sent['copy_id']

create (m:marketing {name: "Nov Certificate", type: "Mail"})
create (m:marketing {name: "Nov Letter", type: "Mail"})
create (m:marketing {name: "December Letter", type: "Mail"})
create (m:marketing {name: "January Letter", type: "Mail"})

// import mailers
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///mailer/nov-mail.csv" AS mail
MATCH (c:contact {dealid: toInteger(mail['dealid'])}), (m:marketing {name: "Nov Certificate"})
MERGE (c)<-[mk:mrk_message {group: "CRM", date_sent: '2018-11-15'}]-(m)

// import mailers
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///mailer/nov-mail-experian.csv" AS mail
MATCH (c:contact {dealid: toInteger(mail['dealid'])}), (m:marketing {name: "Nov Letter"})
MERGE (c)<-[mk:mrk_message {group: "Experian", date_sent: '2018-11-15'}]-(m)

// import mailers
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///mailer/dec-mail-experien.csv" AS mail
MATCH (c:contact {dealid: toInteger(mail['dealid'])}), (m:marketing {name: "December Letter"})
MERGE (c)<-[mk:mrk_message {group: "Experian", date_sent: '2018-12-15'}]-(m)

// import mailers
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///mailer/dec-mail.csv" AS mail
MATCH (c:contact {dealid: toInteger(mail['dealid'])}), (m:marketing {name: "December Letter"})
MERGE (c)<-[mk:mrk_message {group: "CRM", date_sent: '2018-12-15'}]-(m)

// import mailers
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///mailer/jan-mail-experien.csv" AS mail
MATCH (c:contact {dealid: toInteger(mail['dealid'])}), (m:marketing {name: "January Letter"})
MERGE (c)<-[mk:mrk_message {group: "Experian", date_sent: '2018-12-27'}]-(m)

// import mailers
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///mailer/jan-mail.csv" AS mail
MATCH (c:contact {dealid: toInteger(mail['dealid'])}), (m:marketing {name: "January Letter"})
MERGE (c)<-[mk:mrk_message {group: "CRM", date_sent: '2018-12-27'}]-(m)


// query 
match (m:marketing {type: "email"})-[mk:mrk_message]-(c:contact)-[du:dispo_update]-(d:dispo {type: "sale-sales completed"}) 
where c.salesdate > '2019-01-01' AND c.salesdate < '2019-01-24'
return c.dealid, c.datecr, c.salesdate, c.email, collect(m.name) AS messages, count(mk) as touches

// sales this month
match (c:contact)-[du:dispo_update]-(d:dispo) 
where c.salesdate > '2019-01-01' AND c.salesdate < '2019-01-24' AND (c.status = "6" OR c.status = "7") 
return c.dealid, c.datecr, c.salesdate, c.email, count(d)

// query 
match (c:contact)-[du:dispo_update]-(d:dispo) 
where c.datecr > '2019-01-01' AND c.datecr < '2019-01-24'
return c.dealid, c.datecr, count(du), c.sold, c.cancel, collect(d.type)

// Delete relationships
Match (c:contact)-[mk:mrk_message]-(m:marketing {name: 'Nov Certificate'})
delete m

// Missing attributes
MATCH (n) WHERE NOT EXISTS(n.foo) RETURN n

// Find clients sent marketing message that closed.
Match (d:dispo)-[updates:dispo_update]-(c:contact)-[:mrk_message]-(m:marketing{date: "20190107"})
where c.salesdate > "2019-01-07" AND c.salesdate < "2019-01-09"
with c, m, collect(d) as dispos, count(d) as fullDispos
unwind dispos as dispo
match (dispo)-[]-(c)
where dispo.date > "01/07/2019"
return c.dealid, c.status, c.salesdate, c.datecr, c.dateASAP, c.days, m.name, m.version fullDispos, count(dispo) as after

// Last attribution
MATCH (c:contact)
where c.status = "6" OR c.status = "7"
OPTIONAL MATCH (m:marketing)-[mk:mrk_message]-(c)
WITH c, m, mk
ORDER BY mk.date_sent
RETURN c.dealid as dealid, c.datecr as datecr, c.dateASAP as dateASAP, c.salesdate as SaleDate, c.email as email, count(m) as MessagesSent, last(collect(m.name)) as LastMessage, last(collect(m.type)) as LastType

MATCH (c:contact)
where c.salesdate > '2019-01-01' AND c.salesdate < '2019-01-26' AND (c.status = "6" OR c.status = "7")  
OPTIONAL MATCH (m:marketing)-[mk:mrk_message]-(c)
WITH c, m, mk
ORDER BY mk.date_sent
WITH c, m, last(collect(m)) AS most_recent_c
RETURN c.dealid, c.salesdate, c.email, m.name

// App set
match (d:dispo {type: "sale-sales completed"})-[du:dispo_update]-(c:contact)-[mk:mrk_message]-(m:marketing)
where datetime(du.date) > datetime(mk.date_sent) AND datetime(du.date) < datetime(mk.date_sent) + duration('P7D')
return m.name as Email_Name, count(c) as Opens, m.sent_count as Sent, count(du.date) as App_Set