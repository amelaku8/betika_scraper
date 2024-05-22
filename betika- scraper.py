import requests
import tqdm
import pandas as pd


class BetikaAPI():

    name = "Betika"
    def __init__(self,api_url= 'https://api.betika.co.tz/v1/uo/sports',country_name = None,league_name = None,sport_name = None ):
        self.league_name = league_name
        self.country_name = country_name
        if sport_name == 'Football':
            self.sport_name = 'Soccer'
        else:
            self.sport_name = sport_name
        self.api_url = api_url

    def get_leagues(self):
        leagues_json = requests.get(self.api_url).json()['data']
        self.leagues = []
        for sport in leagues_json:
            for category in sport['categories']:
                for competition in category['competitions']:
                    j = {}
                    j['sport_name'] = sport['sport_name']
                    j['sport_id'] = sport['sport_id']
                    j['country_name'] = category['category_name']
                    j['country_id'] = category['category_id']
                    j['league_name'] = competition['competition_name']
                    j['league_id'] = competition['competition_id']
                    self.leagues.append(j)
        self.leagues = pd.DataFrame(self.leagues)
        if self.league_name:
            self.leagues = self.leagues[self.leagues['league_name'] == self.league_name]
        if self.country_name:
            self.leagues = self.leagues[self.leagues['country_name'] == self.country_name]
        if self.sport_name:
            self.leagues = self.leagues[self.leagues['sport_name'] == self.sport_name]

    def get_matches(self):
        self.get_leagues()
        i = {}
        for competition_id in tqdm.tqdm(self.leagues['league_id']):
            response = requests.get(
                f'https://api.betika.co.tz/v1/uo/matches?&competition_id={competition_id}&tab=upcoming&sub_type_id=1,186&country_id=3&language=en')
            i[competition_id] = response.json()
            ll = []
            for league in i:
                matches = i[league]['data']
                for match in matches:
                    z = {}
                    z['match_id'] = match['match_id']
                    z['game_id'] = match['game_id']
                    z['home_team'] = match['home_team']
                    z['away_team'] = match['away_team']
                    z['start_time'] = match['start_time']
                    z['league_name'] = match['competition_name']
                    z['sport_name'] = match['sport_name']
                    z['country_name'] = match['category']
                    ll.append(z)
        self.matches = pd.DataFrame(ll)

    def get_odds(self):
        self.get_matches()
        odds = {}
        for matchid in tqdm.tqdm(self.matches['match_id']):
            try:
                params = {
                    'id': matchid,
                '   language': 'en'
                }
                response = requests.get('https://api.betika.co.tz/v1/uo/match', params=params)
                odds[matchid] = response.json()['data']
            except requests.exceptions.ConnectionError:
                continue
        dff = []
        for match in odds:
            kk = {}
            kk['match_id'] = match
            odds_list = odds[match]
            if odds_list != []:

                if type(odds_list) != type(None):
                    for odd in odds_list:
                        for values in odd['odds']:
                            
                            name = values['odd_def'].replace('{$competitor1}', 'Home').replace('{$competitor2}', 'Away') + values['special_bet_value']
                            kk[odd['name'] + name] = values['odd_value']
                            

            dff.append(kk)
            self.odds = self.matches.merge(pd.DataFrame(dff),on = 'match_id')
        return self.odds

  

provider = BetikaAPI()
data = provider.get_odds()
data.to_csv('betika_matches_and_odds.csv')





