from lxml import etree
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import glob

# -----------------------------
# Load speaker metadata
# -----------------------------
def load_speaker_metadata(metadata_file):
    tree = etree.parse(metadata_file)
    ns = {"tei": "http://www.tei-c.org/ns/1.0"}
    speakers = {}
    for person in tree.xpath("//tei:person", namespaces=ns):
        speaker_id = person.get("{http://www.w3.org/XML/1998/namespace}id")
        name = " ".join(person.xpath(".//tei:persName//text()", namespaces=ns))
        gender = person.xpath(".//tei:sex/@value", namespaces=ns)
        gender = gender[0] if gender else None

        party_ref = person.xpath(".//tei:affiliation[@role='member']/@ref", namespaces=ns)
        party_id = party_ref[0].lstrip("#") if party_ref else None
        speakers[speaker_id] = {"name": name, "gender": gender, "party": party_id}
    return speakers

# -----------------------------
# Load political orientation
# -----------------------------
def load_political_orientation(taxonomy_file):    
    tree = etree.parse(taxonomy_file)
    NS = {"tei": "http://www.tei-c.org/ns/1.0"}

    orientation_map = {}

    for cat in tree.xpath("//tei:category", namespaces=NS):
        oid = cat.get("{http://www.w3.org/XML/1998/namespace}id")
        term = cat.xpath("string(.//tei:term)", namespaces=NS)
        orientation_map[oid] = term
        print(term)
    return orientation_map

# -----------------------------
# Load organization metadata
# -----------------------------
def load_org_metadata(org_file, taxonomy_map):
    tree = etree.parse(org_file)
    ns = {"tei": "http://www.tei-c.org/ns/1.0"}

    orgs = {}
    orgs_type = {}
    party_orientation = {}  

    for org in tree.xpath("//tei:org", namespaces=ns):
        org_id = org.get("{http://www.w3.org/XML/1998/namespace}id")
        org_name = org.xpath("string(.//tei:orgName[@full='yes'])", namespaces=ns)
        role = org.get("role")
        orgs[org_id] = org_name
        orgs_type[org_id] = role
    
        if role == "politicalParty":
            orientation_id = org.xpath("string(.//tei:state[@type='politicalOrientation']/tei:state/@ana)", namespaces=ns)
            # remove leading '#' if present
            orientation_id = orientation_id.lstrip("#") if orientation_id else None
            #print (orientation_id)
            # map to readable name
            orientation_name = taxonomy_map.get(orientation_id)
            #print (orientation_name)
            party_orientation[org_id] = orientation_name

    return orgs, orgs_type, party_orientation

# -----------------------------
# Extract speeches from debate XML
# -----------------------------
def extract_speeches(xml_file):
    tree = etree.parse(xml_file)
    ns = {"tei": "http://www.tei-c.org/ns/1.0"}

    # Get debate title (head of debateSection)
    divs = tree.xpath("//tei:div[@type='debateSection']", namespaces=ns)
    speeches = []

    for div in divs:
        debate_title = div.findtext("tei:head", namespaces=ns)

        for u in div.xpath(".//tei:u", namespaces=ns):
            speaker_ref = u.get("who")
            if speaker_ref:
                speaker_ref = speaker_ref.replace("#", "")
            speech_type = u.get("ana")
            text = " ".join(u.xpath(".//tei:seg//text()", namespaces=ns)).strip()
            if not text:
                continue
            speeches.append({
                "speaker_id": speaker_ref,
                "speech_type": speech_type,
                "debate_title": debate_title,
                "text": text                
            })
   
    date_elem = tree.xpath("//tei:fileDesc//tei:sourceDesc//tei:date/@when", namespaces=ns)
    if date_elem:
        speech_date = date_elem[0]
        year = speech_date[:4]
    else:
        year = None
        speech_date = None

    for s in speeches:
        s["date"] = speech_date
        s["year"] = year

    return speeches

# -----------------------------
# File paths
# -----------------------------
metadata_file = r"D:\download\ParlaMint-GB\ParlaMint-GB.TEI\ParlaMint-GB-listPerson.xml"
org_file = r"D:\download\ParlaMint-GB\ParlaMint-GB.TEI\ParlaMint-GB-listOrg.xml"
xml_path = r"D:\download\ParlaMint-GB\ParlaMint-GB.TEI\**\*.xml"
taxonomy_file = r"D:\download\ParlaMint-GB\ParlaMint-GB.TEI\ParlaMint-taxonomy-politicalOrientation.xml"
# -----------------------------
# Load metadata
# -----------------------------
speakers = load_speaker_metadata(metadata_file)
taxonomy_map = load_political_orientation(taxonomy_file)
orgs, orgs_type, party_orientation = load_org_metadata(org_file, taxonomy_map)
# -----------------------------
# Find XML files (exclude metadata/taxonomy)
# -----------------------------
all_files = glob.glob(xml_path, recursive=True)
exclude_keywords = ["listPerson", "listOrg", "taxonomy"]
xml_files = [f for f in all_files]
print(f"Found {len(xml_files)}\n")

# -----------------------------
# Extract speeches
# -----------------------------
all_data = []
for xml_file in tqdm(xml_files, desc="Processing XML files"):
    try:
        all_data.extend(extract_speeches(xml_file))
    except Exception as e:
        print("Error parsing:", xml_file, e)

# -----------------------------
# Create DataFrame and add metadata
# -----------------------------
df = pd.DataFrame(all_data)

# Add speaker info
df["speaker_name"] = df["speaker_id"].apply(lambda x: speakers.get(x, {}).get("name"))
df["gender"] = df["speaker_id"].apply(lambda x: speakers.get(x, {}).get("gender"))
df["party_id"] = df["speaker_id"].apply(lambda x: speakers.get(x, {}).get("party"))
df["party_id"] = df["party_id"].str.lstrip("#")

df["party_name"] = df["party_id"].map(orgs)
df["chamber"] = df["party_id"].map(orgs_type)
df["political_orientation"] = df["party_id"].map(party_orientation)

print("Done!")
# -----------------------------
# Save CSV
# -----------------------------
output_file = "D:/download/ParlaMint-GB/full_data_parlamint_gb.csv"
df.to_csv(output_file, index=False, encoding="utf-8-sig")
print("Saved CSV to:", output_file)

print(df.head())
