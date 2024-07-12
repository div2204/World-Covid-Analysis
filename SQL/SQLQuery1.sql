select *
from Covid..CovidDeaths$
where continent is not null
order by 3,4;

--Main Dataset

select location, date, total_cases, new_cases, total_cases, population from Covid..CovidDeaths$
where continent is not null
order  by 1,2;

-- Total Population
select sum(population)
from (select distinct location, population from Covid..CovidDeaths$ where continent is not null) as pop


--Total Case v/s Total Death

select location, date, total_cases,total_deaths,round((total_deaths/total_cases)*100,2) as percentage_deaths_in_infected 
from Covid..CovidDeaths$
where continent is not null
order  by 1,2;

--Total Cases v/s Population

select location, date, total_cases, round((total_cases/population)*100,3) as percentage_population_infected
from Covid..CovidDeaths$
where continent is not null
--where location like '%Ind%'
order  by 1,2;

--WORLD

select sum(new_cases) as Total_Cases, sum(CONVERT(int, Covid..CovidDeaths$.new_deaths)) as Total_Deaths, round((sum(CONVERT(int, Covid..CovidDeaths$.new_deaths))/sum(new_cases))*100,3) as Death_Percentage
from Covid..CovidDeaths$
where continent is not null 
--where location like '%Ind%'
order  by 1,2;


--Countries with highest infection rate

select location, population, max(total_cases) as max_infected , max(round((total_cases/population)*100,3)) as percentage_population_infected
from Covid..CovidDeaths$
where continent is not null
GROUP BY location, population 
--where location like '%Ind%'
order  by 4 desc;

--Countries with highest infection rate with date

select location, population, date, max(total_cases) as max_infected , max(round((total_cases/population)*100,3)) as percentage_population_infected
from Covid..CovidDeaths$
where continent is not null
GROUP BY location, population, date 
--where location like '%Ind%'
order  by 1;


-- Countries with total death count per population

select location, max(cast(total_deaths as int)) as total_deaths_count
from Covid..CovidDeaths$
where continent is not null
GROUP BY location, population 
--where location like '%Ind%'
order  by total_deaths_count desc;


-- Continent with total death count per population
 select location, max(cast(total_deaths as int)) as total_deaths_count
from Covid..CovidDeaths$
where continent is null and location not in ('World', 'European Union', 'International') 
GROUP BY  location
--where location like '%Ind%'
order  by total_deaths_count desc;


--Global Analysis
select date, sum(new_cases) as global_new_cases, SUM(cast(new_deaths as int)) as global_new_deaths,
round((SUM(cast(new_deaths as int))/SUM(new_cases)*100),2) as percentage_deaths
from Covid..CovidDeaths$
where continent is not null
group by date
order  by 1,2;	



-- Population v/s Vaccinations
--Country wise
WITH VaccinationRollup AS (
    SELECT 
        dea.continent,
        dea.location, 
        dea.date,
        SUM(CONVERT(BIGINT, COALESCE(vac.people_vaccinated, 0))) OVER (PARTITION BY dea.location ORDER BY dea.date) AS rolling_people_vaccinated,
        SUM(CONVERT(BIGINT, COALESCE(vac.people_fully_vaccinated, 0))) OVER (PARTITION BY dea.location ORDER BY dea.date) AS rolling_people_fully_vaccinated
    FROM 
        Covid..CovidDeaths$ AS dea
    JOIN 
        Covid..CovidVaccinations$ AS vac 
        ON vac.location = dea.location
        AND dea.date = vac.date 
    WHERE 
        dea.continent IS NOT NULL
)
SELECT 
    continent,
    location,
    MAX(rolling_people_vaccinated) AS max_rolling_people_vaccinated,
    MAX(rolling_people_fully_vaccinated) AS max_rolling_people_fully_vaccinated
FROM 
    VaccinationRollup
WHERE
    rolling_people_vaccinated != 0 AND rolling_people_fully_vaccinated != 0
GROUP BY 
    continent,
    location
ORDER BY 
    location;



-- WOrld People Vaccinated


WITH VaccinationRollup AS (
    SELECT 
        dea.date, 
        SUM(CONVERT(BIGINT, COALESCE(vac.people_vaccinated, 0))) OVER (PARTITION BY dea.location ORDER BY dea.date) AS rolling_people_vaccinated,
        SUM(CONVERT(BIGINT, COALESCE(vac.people_fully_vaccinated, 0))) OVER (PARTITION BY dea.location ORDER BY dea.date) AS rolling_people_fully_vaccinated
    FROM 
        Covid..CovidDeaths$ AS dea
    JOIN 
        Covid..CovidVaccinations$ AS vac 
        ON vac.location = dea.location
        AND dea.date = vac.date 
    WHERE 
        dea.continent IS NOT NULL
)
SELECT 
    date,
    SUM(rolling_people_vaccinated) AS total_rolling_people_vaccinated,
    SUM(rolling_people_fully_vaccinated) AS total_rolling_people_fully_vaccinated
FROM 
    VaccinationRollup
GROUP BY 
    date
ORDER BY 
    date;





-- CTE 

with PopvsVac (continent, location, date, population, new_vaccinations, rolling_toal_vaccinations)

as

(select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
sum(convert(int,vac.new_vaccinations)) over (partition by dea.location order by dea.location, dea.date) as rolling_total_vaccinations
from Covid..CovidDeaths$ as dea
join Covid..CovidVaccinations$ as vac 
on vac.location = dea.location
and dea.date = vac.date 
 where dea.continent is not null
--order by 2,3
)

select*, (rolling_toal_vaccinations/population)*100
from PopvsVac
order by population desc




--TEMP table
Drop Table if exists #PopluationvsVaccination
create table #PopluationvsVaccination(
Continent varchar(255),
Location varchar (255),
Date datetime,
population numeric,
New_vaccination numeric,
Rolling_total_vaccinations numeric
)

insert into #PopluationvsVaccination
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
sum(convert(int,vac.new_vaccinations))
over (partition by dea.location order by dea.location, dea.date) as rolling_total_vaccinations
from Covid..CovidDeaths$ as dea
join Covid..CovidVaccinations$ as vac 
on vac.location = dea.location
and dea.date = vac.date 
 where dea.continent is not null
--order by 2,3

--Vaccination World Anlaysis
select Date, Sum(new_vaccination) as new_vac, sum(rolling_total_vaccinations) as rolling_sum_vac 
from #PopluationvsVaccination
group by Date
order by Date


--create view 
drop view if exists PopluationvsVaccination
create view PopluationvsVaccination as 
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, 
sum(convert(int,vac.new_vaccinations))
over (partition by dea.location order by dea.location, dea.date) as rolling_total_vaccinations
from Covid..CovidDeaths$ as dea
join Covid..CovidVaccinations$ as vac 
on vac.location = dea.location
and dea.date = vac.date 
 where dea.continent is not null


