from distutils.core import setup

setup(
    name='aprs2influxdb',
    version='0.2.1',
    author='Bryce Salmi',
    author_email='Bryce@FaradayRF.com',
    packages=['aprs2influxdb'],
    url='https://github.com/FaradayRF/aprs2influxdb',
    license='GPLv3',
    description='Interfaces ham radio APRS-IS servers and saves packet data into an influxdb database',
    long_description=open('README.md').read(),
    install_requires=[
        "aprslib>=0.6.46",
        "certifi>=2017.7.27.1",
        "chardet>=3.0.4",
        "configparser>=3.5.0",
        "idna>=2.6",
        "influxdb>=4.1.1",
        "pbr>=3.1.1",
        "python-dateutil>=2.6.1",
        "pytz>=2017.2",
        "requests>=2.18.4",
        "six>=1.11.0",
        "urllib3>=1.22",
    ],
    entry_points={
        'console_scripts':
            ['aprs2influxdb = aprs2influxdb.__main__:main']
    }
)
