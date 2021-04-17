import requests
import json
import pandas
import bs4


class AFA_Scraper:
    def __init__(self, id, competition):
        self.id = id
        self.comp = competition

    def get_data(self):
        url1 = 'https://info.afa.org.ar/deposito/html/v3/' \
            'htmlCenter/data/deportes/futbol/{0}/events/{1}.json'

        url = url1.format(self.comp, str(self.id))
        r = requests.request('GET', url)
        self.data = json.loads(r.text)

    def match(self):
        data = self.data['match']
        stad = self.data['venueInformation']['venue']['stadium']
        a = [{
            'matchId': self.id,
            'homeId': data['homeTeamId'],
            'awayId': data['awayTeamId'],
            'yyymmd': data['date'],
            'hhmm': data['scheduledStart'],
            'fecha': data['week'],
            'stadium': stad['stadiumName'],
            'city': stad['state']
        }]

        return pandas.DataFrame(a)

    def teams(self):
        goles = self.data['scoreStatus']
        teams_id = goles.keys()

        a = {'matchId': self.id,
             'teamId': [int(i) for i in teams_id],
             'teamName': [self.data['teams'][i]['name'] for i in teams_id],
             'home': [1, 0],
             'score': [goles[i]['score'] for i in teams_id],
             'Posession': [self.data['summary']['ballPossesion']['homeQty'],
                           self.data['summary']['ballPossesion']['awayQty']]}

        return pandas.DataFrame(a)

    def instances(self):
        plyrId = self.data['players'].keys()
        data = [self.data['players'][i] for i in plyrId]
        jog = pandas.json_normalize(data)

        pplay = self.data['summary'].copy()
        pplay.pop('ballPossesion')
        pplay.pop('playersPasses')

        df_list = []

        for i in pplay.keys():
            player = pplay[i]['perPlayerQty'].keys()
            instances = pplay[i]['perPlayerQty'].values()
            d = {'plyrId': player, i: instances}
            df = pandas.DataFrame(d)
            df_list.append(df)

        df_list = [df.set_index('plyrId') for df in df_list]
        all_instances = pandas.concat(df_list, axis=1).reset_index()
        all_instances.rename(columns={'index': 'plyrId'}, inplace=True)
        all_instances = all_instances.fillna(0)
        jog['plyrId'] = plyrId
        jog['matchId'] = self.id

        final = pandas.merge(all_instances, jog, 'outer', on='plyrId')
        return final


def get_match_id(competition):
    link = 'https://info.afa.org.ar/deposito/html/' \
        'v3/htmlCenter/data/deportes/futbol/{}/pages/es/fixture.html'
    url = link.format(competition)
    r = requests.request('GET', url)
    soup = bs4.BeautifulSoup(r.text)
    matches = soup.find_all(attrs={"data-channel": True})

    def dig(x):
        return int(''.join(list(filter(str.isdigit, x))))

    return [dig(i.get('data-channel')) for i in matches]


def main():
    primeraa = get_match_id('primeraa')
    matches = []
    instances = []
    teams = []

    for i in primeraa:
        juego = AFA_Scraper(i, 'primeraa')
        juego.get_data()
        matches.append(juego.match())
        instances.append(juego.instances())
        teams.append(juego.teams())

    a = pandas.concat(matches)
    b = pandas.concat(teams)
    c = pandas.concat(instances)

    dteams = b[['teamId', 'teamName']].drop_duplicates()

    auxlist = [
        'plyrId',
        'name.first',
        'name.middle',
        'name.last',
        'name.nick',
        'name.shortName'
    ]

    dplayers = c[auxlist].drop_duplicates()
    dplayers.columns = dplayers.columns.str.replace("name.", "")

    b = b.drop('teamName', 1)
    auxlist2 = [
        'name.first',
        'name.middle',
        'name.nick',
        'name.last',
        'name.shortName',
        'gender',
        'order'
    ]
    c = c.drop(auxlist2, 1)

    a.to_csv('../data/matches.csv', index=False)
    b.to_csv('../data/results.csv', index=False)
    c.to_csv('../data/instances.csv', index=False)

    dteams.to_csv('../data/team_dict.csv', index=False)
    dplayers.to_csv('../data/player_dict.csv', index=False)


if __name__ == '__main__':
    main()
