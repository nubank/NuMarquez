import ReactGA from 'react-ga4';

const initializeGA = () => {
  ReactGA.initialize('G-ZVKMSGZ7J4');
  ReactGA.send({ hitType: 'pageview', page: window.location.pathname });
  ReactGA.event({ category: 'User', action: 'Visited' });
};

export default initializeGA;