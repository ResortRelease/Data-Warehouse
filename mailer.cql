// Import the latest data
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///export-mk.csv" AS contact
MERGE (c:contact {dealid: contact['dealid']})
ON match set c.salesdate = contact['salesdate'], c.datecr = contact['datecr'], c.dateASAP = contact['dateASAP'], c.days = toInteger(contact['days'])

// Find clients sent marketing message that closed.
Match (d:dispo)-[updates:dispo_update]-(c:contact)-[:mrk_message]-(m:marketing{type: "Mailing"})
where c.salesdate > "2018-11-27" AND c.salesdate < "2018-12-21"
with c, m, collect(d) as dispos, count(d) as fullDispos
unwind dispos as dispo
match (dispo)-[]-(c)
where dispo.date > "11/27/2018"
return c.dealid, c.status, c.salesdate, c.datecr, c.dateASAP, c.days, m.name, fullDispos, count(dispo) as after