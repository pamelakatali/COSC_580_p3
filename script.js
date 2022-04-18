const APIURL = 'https://api.github.com/users/'

const main = document.getElementById('main')
const form = document.getElementById('form')
const search = document.getElementById('search')

async function getUser(username) {
    try {
        const { data } = await axios(APIURL + username)

        createUserCard(data)
        getRepos(username)
    } catch(err) {
        if(err.response.status == 404) {
            createErrorCard('No profile with this username')
        }
    }
}


async function parseSQL(sql_str) {
	var sql_data = sql_str
	console.log(sql_data)
	$.ajax(
	    {
	        type:'POST',
	        contentType:'application/json;charset-utf-08',
	        dataType:'json',
	        url:'http://127.0.0.1:5000/pass_val?value='+sql_data ,
	        success:function (data) {
	            var reply = data.reply;
                console.log(data)
	            if (reply == "success")
	            {
                    console.log(data.result)
	                return;
	            }
	            else
	                {
	                alert("some error ocured in session agent")
	                }

	        }
	    }
	);
}

async function getRepos(username) {
    try {
        const { data } = await axios(APIURL + username + '/repos?sort=created')

        addReposToCard(data)
    } catch(err) {
        createErrorCard('Problem fetching repos')
    }
}

function createUserCard(user) {
    const userID = user.name || user.login
    const userBio = user.bio ? `<p>${user.bio}</p>` : ''
    const cardHTML = `
    <div class="card">
    <div>
      <img src="${user.avatar_url}" alt="${user.name}" class="avatar">
    </div>
    <div class="user-info">
      <h2>${userID}</h2>
      ${userBio}
      <ul>
        <li>${user.followers} <strong>Followers</strong></li>
        <li>${user.following} <strong>Following</strong></li>
        <li>${user.public_repos} <strong>Repos</strong></li>
      </ul>
      <div id="repos"></div>
    </div>
  </div>
    `
    main.innerHTML = cardHTML
    
}

function createErrorCard(msg) {
    const cardHTML = `
        <div class="card">
            <h1>${msg}</h1>
        </div>
    `

    main.innerHTML = cardHTML
}

function addReposToCard(repos) {
    const reposEl = document.getElementById('repos')

    repos
        .slice(0, 5)
        .forEach(repo => {
            const repoEl = document.createElement('a')
            repoEl.classList.add('repo')
            repoEl.href = repo.html_url
            repoEl.target = '_blank'
            repoEl.innerText = repo.name

            reposEl.appendChild(repoEl)
        })
}

form.addEventListener('submit', (e) => {
    e.preventDefault()

    const sql_str = search.value

    if(sql_str) {
        parseSQL(sql_str)

        search.value = ''
    }
})



/*


// Get the button and container elements from HTML:
const button = document.getElementById("theButton")
const data = document.getElementById("info")


// Create an array of cars to send to the server:
const cars = [
 { "make":"Porsche", "model":"911S" },
 { "make":"Mercedes-Benz", "model":"220SE" },
 { "make":"Jaguar","model": "Mark VII" }
];


// Create an event listener on the button element:
button.onclick= function() {
	// Get the reciever endpoint from Python using fetch:
	 fetch("http://127.0.0.1:5000/receiver", 
	 {
	 	method: 'POST',
	 	headers: { 
	 	'Content-type': 'application/json',
	 	'Accept': 'application/json'
	 	},

	 	// Strigify the payload into JSON:
	 	body:JSON.stringify(cars)}).then(res=> {
		 	if(res.ok) {
		 		return res.json()
		 	} else {
		 	alert("something is wrong")
		 	}
	 	}).then(jsonResponse=> {
	 
		// Log the response data in the console
		console.log(jsonResponse)
	 	} 
	 ).catch((err) => console.error(err));
 
 	}

*/