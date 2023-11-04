import {fetchConfig, fetchSnapshotsPath} from "../utils/PlakarApiClient";

export const fetchInitialData = () => async dispatch => {
    dispatch({type: 'FETCH_INITIAL_DATA_REQUEST'});
    try {
        // Fetch the initial data from the API
        const response = await fetch('/api/initial-data');
        const data = await response.json();
        // Dispatch the success action with the data
        dispatch({type: 'FETCH_INITIAL_DATA_SUCCESS', payload: data});
    } catch (error) {
        // Dispatch the failure action with the error
        dispatch({type: 'FETCH_INITIAL_DATA_FAILURE', error});
    }
};

// Example action
export const fetchSnapshots = () => async dispatch => {
    dispatch({type: 'FETCH_SNAPSHOTS_REQUESTS'});
    try {
        // sleep for 3 seconds to simluate a slow network
        await new Promise(r => setTimeout(r, 3000));
        //   const response = await fetch('/api/posts');
        //   const data = await response.json();
        const data = [
            {'uuid': '435FEAC9-7FFC-45B1-8E9C-6122DF2C953D', 'hostname': 'poolp.local'},
            {'uuid': '806C7584-2488-4F39-A639-CF617C5694C7', 'hostname': 'dummy.local'},
            {'uuid': '90B077B5-1625-484F-8B23-D6D0B2A192AF', 'hostname': 'fred.local'},
        ];
        dispatch({type: 'FETCH_SNAPSHOTS_SUCCESS', payload: data});
    } catch (error) {
        dispatch({type: 'FETCH_SNAPSHOTS_FAILURE', error});
    }
};

const confState = {
    apiUrl: null,
    repository: null,
    loading: false,
    error: null,
};
export const confReducer = (state = confState, action) => {
    switch (action.type) {
        case 'SET_API_URL':
            return {...state, apiUrl: action.payload.apiUrl};
        case 'FETCH_CONF_REQUEST':
            return {...state, loading: true};
        case 'FETCH_CONF_SUCCESS':
            return {...state, loading: false, repository: action.payload.repository};
        case 'FETCH_CONF_FAILURE':
            return {...state, loading: false, error: action.error, repository: null};
        default:
            return state;
    }
};

export const confApp = (apiUrl) => async dispatch => {
    dispatch({type: 'SET_API_URL', payload: {apiUrl: apiUrl}});
    dispatch({type: 'FETCH_CONF_REQUEST'});
    try {
        // sleep for 3 seconds to simluate a slow network
        await fetchConfig(apiUrl).then((data) => {
            dispatch({type: 'FETCH_CONF_SUCCESS', payload: data});
        });
    } catch (error) {
        dispatch({type: 'FETCH_CONF_FAILURE', error});
    }
};

// Example reducer
const initialState = {
    snapshots: [],
    loading: false,
    error: null,
};

export const snapshotsReducer = (state = initialState, action) => {
    switch (action.type) {
        case 'FETCH_SNAPSHOTS_REQUESTS':
            return {...state, loading: true};
        case 'FETCH_POSTS_SUCCESS':
            return {...state, loading: false, posts: action.payload};
        case 'FETCH_SNAPSHOTS_FAILURE':
            return {...state, loading: false, error: action.error};
        default:
            return state;
    }
};


// Example selector
export const selectSnapshots = glState => glState.snapshots;
export const selectConf = glState => glState.conf;

export const selectSnapshot = glState => glState.pathView.snapshot;
export const selectFileDetails = glState => glState.pathView.items[0];
export const selectPathPage = glState => glState.pathView;

const pathViewState = {
    snapshot: null,
    items: [],
    page: 1,
    pageSize: 10,
    totalPages: 1,
    loading: false,
    error: null,
}

export const pathViewReducer = (state = pathViewState, action) => {
        switch (action.type) {
            case 'FETCH_PATH_REQUEST':
                return {...state, loading: true, snapshot: null, items: []};
            case 'FETCH_PATH_SUCCESS':
                return {
                    ...state,
                    loading: false,
                    snapshot: action.payload.snapshot,
                    items: action.payload.items,
                    page: action.payload.page,
                    pageSize: action.payload.pageSize,
                    totalPages: action.payload.totalPages,
                }
            case 'FETCH_PATH_FAILURE':
                return {...state, loading: false, error: action.error};
            default:
                return state;
        }
    }
;

export const fetchPath = ({snapshotId, path, page = 1, pageSize = 10}) => async dispatch => {
    dispatch({type: 'FETCH_PATH_REQUEST'});
    try {
        console.log('fetchPath', {snapshotId, path});
        // sleep for 3 seconds to simluate a slow network
        await fetchSnapshotsPath('', `${snapshotId}:${path}`, 1, 10).then((page) => {
            console.log('file data', page.items[0]);
            console.log('snapshot', page.snapshot);
            dispatch({type: 'FETCH_PATH_SUCCESS', payload: page});
        });
    } catch (error) {
        console.log('Error:', error);
        dispatch({type: 'FETCH_PATH_FAILURE', error});
    }
};