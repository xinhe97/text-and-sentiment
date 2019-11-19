
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import xml.etree.ElementTree as ET


class event:
    def __init__(self, xml_file):
        self.tree = ET.parse(xml_file)
        self.root = self.tree.getroot()
        self.ticker = 'ticker'
        self.eventdate = 'date'
        self.title = 'title'
        self.editdate = 'date'
        
    def get_elements(self):
        self.ticker = self.root.findtext('companyTicker')
        self.city = self.root.findtext('city')
        self.companyname = self.root.findtext('companyName')
        self.eventdate = self.root.findtext('startDate')
        self.title = self.root.findtext('eventTitle')
        
        self.editdate = self.root.get('lastUpdate')
        self.typeid = self.root.get('eventTypeId')
        self.id = self.root.get('Id')
        self.typename = self.root.get('eventTypeName')
        
        self.headline = self.root[0][0].text
        self.story = self.root[0][1].text
        
        self.fakeqtr = self.title[:7]
        
        return(
        'typeid is '+self.typeid+'\n'+
        'ticker is '+self.ticker+'\n'+
        'date is '+self.eventdate+'\n'+
        'title is '+self.title+'\n'+
        'fake qtr is '+self.fakeqtr+'\n'
        )
    
    def tab(self):
        mylist = [
                self.typeid,
                self.ticker,
                self.eventdate,
                self.fakeqtr,
                self.city,
                self.companyname
                ]
        return(mylist)
        
    def run(self):
        print(self.get_elements())

if __name__ == "__main__":
    file_path = './xml-data/2002/516024_T.xml'
    e1 = event(file_path)
    print('\n')
    print(e1.get_elements())
    print('\n')
    print(e1.tab())
