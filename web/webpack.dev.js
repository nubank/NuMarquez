const { merge } = require('webpack-merge')
const webpack = require('webpack')
const webpackShared = require('./webpack.common.js')
const CopyPlugin = require('copy-webpack-plugin')
const HtmlWebpackPlugin = require('html-webpack-plugin')
const path = require('path')

// look for elkjs package folder
const elkjsRoot = path.dirname(require.resolve('elkjs/package.json'));

const webpackDev = {
  mode: 'development',
  devServer: {
    static: {
      directory: __dirname + '/src',
      staticOptions: {},
      publicPath: "/",
      serveIndex: true,
      watch: true,
    },
    port: 3000,
    devMiddleware: {
      publicPath: '/'
    },
    historyApiFallback: {
      index: './index.html',
      disableDotRule: true
    },
    proxy: {
      '/api': {
        target: `http://${process.env.MARQUEZ_HOST || 'localhost'}:${process.env.MARQUEZ_PORT || 5000}/`,
        secure: false,
        logLevel: 'debug',
        headers: {
          'X-Bifrost-Authentication': 'developer'
        }
      }
    }
  },
  // Enable sourcemaps for debugging webpack"s output.
  devtool: 'eval-cheap-module-source-map',
  plugins: [
    new webpack.DefinePlugin({
      'process.env': {
        REACT_APP_OKTA_ISSUER: JSON.stringify(process.env.REACT_APP_OKTA_ISSUER),
        REACT_APP_OKTA_CLIENT_ID: JSON.stringify(process.env.REACT_APP_OKTA_CLIENT_ID),
      },
      __DEVELOPMENT__: JSON.stringify(true),
      __REACT_APP_ADVANCED_SEARCH__: true,
      __API_URL__: JSON.stringify('/api/v1'),
      __API_BETA_URL__: JSON.stringify('/api/v2beta'),
      __NODE_ENV__: JSON.stringify('development'),
      __TEMP_ACTOR_STR__: JSON.stringify('me'),
      __FEEDBACK_FORM_URL__: JSON.stringify('https://forms.gle/f3tTSrZ8wPj3sHTA7'),
      __API_DOCS_URL__: JSON.stringify('https://marquezproject.github.io/marquez/openapi.html')
    }),
      new CopyPlugin({
        patterns: [
          { from: path.join(elkjsRoot, 'lib/elk-worker.min.js'), to: 'elk-worker.min.js' },
        ],
      }),
      new HtmlWebpackPlugin({
        template: './src/index.html',
        filename: 'index.html',
        inject: true
      })
  ]
}

module.exports = merge(webpackShared, webpackDev)
