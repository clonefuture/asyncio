import asyncio
import datetime
import aiohttp
from more_itertools import chunked
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker
import requests

CHUNK_SIZE = 10
NUM_OF_CHARACTERS = 50

PG_DSN = 'postgresql+asyncpg://app:1234@127.0.0.1:5431/starwars'
engine = create_async_engine(PG_DSN)
Base = declarative_base()


class People(Base):
    __tablename__ = 'characters'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    birth_year = Column(String, nullable=False)
    eye_color = Column(String, nullable=False)
    films = Column(String, nullable=False)
    gender = Column(String, nullable=False)
    hair_color = Column(String, nullable=False)
    height = Column(String, nullable=False)
    homeworld = Column(String, nullable=True)
    mass = Column(String, nullable=True)
    skin_color = Column(String, nullable=False)
    species = Column(String, nullable=True)
    starships = Column(String, nullable=True)
    vehicles = Column(String, nullable=True)


async def get_character(session, character_id):
    result = await session.get(f'https://swapi.dev/api/people/{character_id}')
    return await result.json()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()
    async_session_maker = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with aiohttp.ClientSession() as web_session:
        for chuk_id in chunked(range(1, NUM_OF_CHARACTERS + 1), CHUNK_SIZE):
            coros = [get_character(web_session, i) for i in chuk_id]
            result = await asyncio.gather(*coros)

            people_list = [People(birth_year=item['birth_year'],
                                  eye_color=item['eye_color'],
                                  films=', '.join([requests.get(i).json()['title'] for i in item['films']]),
                                  gender=item['gender'],
                                  hair_color=item['hair_color'],
                                  height=item['height'],
                                  homeworld=requests.get(item['homeworld']).json()['name'],
                                  mass=item['mass'],
                                  name=item['name'],
                                  skin_color=item['skin_color'],
                                  species=', '.join([requests.get(i).json()['name'] for i in item['species']]),
                                  starships=', '.join([requests.get(i).json()['name'] for i in item['starships']]),
                                  vehicles=', '.join([requests.get(i).json()['name'] for i in item['vehicles']])

                                  ) for item in result if len(item) == 16
                           ]
            async with async_session_maker() as orm_session:
                orm_session.add_all(people_list)
                await orm_session.commit()



start = datetime.datetime.now()


async def tasks():
    await main()


asyncio.get_event_loop().run_until_complete(tasks())

print(datetime.datetime.now() - start)
