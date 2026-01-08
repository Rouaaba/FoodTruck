import React from "react";

export default function Intro() {
  return (
    <div className="intro">
      <h3>About</h3>
      <p style={{color: '#fefefe'}}>
        This is a fun application built to accompany the{" "}
        <a href="http://prakhar.me/docker-curriculum">docker curriculum</a> - a
        comprehensive tutorial on getting started with Docker targeted
        especially at beginners.
      </p>
      <p style={{color: '#fefefe'}}>
        The app is built with Flask on the backend and Elasticsearch is the
        engine powering the search.
      </p>
      <p style={{color: '#fefefe'}}>
        The frontend is hand-crafted with React and the beautiful maps are
        courtesy of Mapbox.
      </p>
      <p style={{color: '#fefefe'}}>
        If you find the design a bit ostentatious, blame{" "}
        <a href="http://genius.com/Justin-bieber-baby-lyrics">Genius</a> for
        giving me the idea of using this color scheme. If you love it, I smugly
        take all the credit. ⊂(▀¯▀⊂)
      </p>
      <p style={{color: '#fefefe'}}>
        Lastly, the data for the food trucks is made available in public domain
        by{" "}
        <a href="https://data.sfgov.org/Economy-and-Community/Mobile-Food-Facility-Permit/rqzj-sfat">
          SF Data
        </a>
      </p>
    </div>
  );
}
