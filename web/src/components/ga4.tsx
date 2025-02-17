import ReactGA from 'react-ga4';

const initializeGA = () => {
  const isStaging = window.location.origin.includes('staging');
  const gaId = isStaging ? 'G-J6G5BV3EV5' : 'G-QF2RHX3HRJ';
  ReactGA.initialize(gaId);
};
  
  const trackPageView = () => {
    ReactGA.send({ hitType: 'pageview', page: window.location.pathname });
  };
  
  const trackEvent = (category: string, action: string, label?: string) => {
    ReactGA.event({ category, action, label });
  };

  export { initializeGA, trackPageView, trackEvent };