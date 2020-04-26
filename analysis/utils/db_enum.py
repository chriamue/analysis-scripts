from enum import Enum


class Gender(Enum):
    male = 1
    female = 2

class Scale3(Enum):
    """For questions like:
    - How high is your body temperature?
    - Age
    """
    low = 0
    mid = 1
    high = 2

class Scale4(Enum):
    """For questions like
    - Have you recently developed difficulty breathing or shortness of breath?
    - Have you recently developed a cough?
    """
    no = 0
    sometimes = 1
    often = 2
    continuous = 3

class Energy(Enum):
    """How are your general energy levels?"""
    normal = 0
    tired = 1
    in_bed_sometimes = 2
    in_bed_often = 3
    in_bed_always = 4

class Exposure(Enum):
    """Have you had close contact with someone infected with coronavirus
    (covid-19)?
    """
    no = 0
    unsure = 1
    yes = 2

class Activity(Enum):
    """What did u do outside?
    """
    nothing = 0
    walking = 1
    running = 2
    cycling = 3
    driving = 4
