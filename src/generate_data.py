import os
import csv
import random
import numpy as np
from faker import Faker
from datetime import datetime, timedelta

# Configuration
TAGS = [
    'cooking', 'games', 'movies', 'technology', 'travel',
    'fitness', 'art', 'music', 'photography', 'nature',
    'fashion', 'health', 'business', 'coding', 'books',
    'sports', 'education', 'diy', 'humor', 'news',
    'pets', 'beauty', 'finance', 'history', 'science'
]

GROUP_NAMES = [
    'The Foodie Collective', 'Indie Game Devs', 'Cinephile Circle', 'Weekend Hikers', 'Remote Work Hub',
    'Startup Founders Lab', 'Digital Art Studio', 'Retro Gaming Lounge', 'Plant Parents United', 'The Bookworm Nook',
    'Global Backpackers', 'Code & Coffee', 'Minimalist Living', 'Fitness Motivation Squad', 'Street Photography Club',
    'Vegan Recipes Exchange', 'Stock Market Watch', 'AI & Future Tech', 'Pet Lovers Society', 'Yoga for Beginners',
    'The Vinyl Records Club', 'Amateur Astronomers', 'DIY Home Decor', 'Creative Writing Workshop', 'Sustainable Living',
    'Mental Health Matters', 'Sci-Fi & Fantasy Fans', 'Car Enthusiasts Network', 'Parenting Support Group', 'Urban Gardening',
    'Solo Travelers', 'Classical Music Appreciation', 'Marathon Runners', 'Board Game Night', 'Crypto & Web3',
    'Fashion Forward', 'History Buffs', 'Philosophy Discussions', 'Interior Design Inspo', 'Chess Masters',
    'Podcast Creators', 'Mountain Bikers', 'Woodworking Projects', 'Language Learners', 'Makeup Artistry',
    'Space Exploration', 'Budget Travel Tips', 'Meditation & Mindfulness', 'E-sports Arena', 'Local Community Events'
]

class DataGenerator:
    def __init__(self, size_preset='small', seed=420):
        self.size_preset = size_preset
        self.seed = seed
        
        random.seed(self.seed)
        np.random.seed(self.seed)
        self.fake = Faker()
        self.fake.seed_instance(self.seed)
        Faker.seed(self.seed)
        
        if size_preset == 'small':
            self.num_users = 525
            self.num_regions = 1
        elif size_preset == 'medium':
            self.num_users = 1050
            self.num_regions = 2
        else: # big
            self.num_users = 10500
            self.num_regions = 10

        self.data_dir = f"data/{size_preset}"
        os.makedirs(self.data_dir, exist_ok=True)

        self.user_ids = list(range(1, self.num_users + 1))
        
        # Cluster Maps: user_id -> cluster_id
        self.user_to_family = {}
        self.user_to_college = {}
        self.user_to_region = {}
        
        # Reverse Maps: cluster_id -> [user_ids]
        self.family_members = {}
        self.college_members = {}
        self.region_members = {}

        self.post_ids = []
        self.lorem_pool = [self.fake.paragraph(nb_sentences=random.randint(1, 5)) for _ in range(1000)]

    def _random_date(self, start_year=2022):
        start = datetime(start_year, 1, 1)
        end = datetime.now()
        return (start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))).strftime('%Y-%m-%d %H:%M:%S')

    def setup_clusters(self):
        """Creates the social graph structure before generating CSVs"""
        print(f"[{self.size_preset}] Mapping social clusters...")
        
        # 1. Assign Regions (Global Pools)
        all_uids = self.user_ids.copy()
        random.shuffle(all_uids)
        
        # Divide users into Regions
        region_chunks = np.array_split(all_uids, self.num_regions)
        for r_id, chunk in enumerate(region_chunks):
            self.region_members[r_id] = chunk.tolist()
            for uid in chunk:
                self.user_to_region[uid] = r_id
            
            # 2. Within each Region, create Colleges (~300 members)
            uids_in_region = chunk.tolist()
            random.shuffle(uids_in_region)
            num_colleges = max(1, len(uids_in_region) // 300)
            college_chunks = np.array_split(uids_in_region, num_colleges)
            
            for c_idx, c_chunk in enumerate(college_chunks):
                c_id = f"R{r_id}_C{c_idx}"
                self.college_members[c_id] = c_chunk.tolist()
                for uid in c_chunk:
                    self.user_to_college[uid] = c_id

            # 3. Within each Region, create Families (~30 members)
            # reshufling region members so families aren't strictly within one college
            random.shuffle(uids_in_region)
            num_families = max(1, len(uids_in_region) // 30)
            family_chunks = np.array_split(uids_in_region, num_families)
            
            for f_idx, f_chunk in enumerate(family_chunks):
                f_id = f"R{r_id}_F{f_idx}"
                self.family_members[f_id] = f_chunk.tolist()
                for uid in f_chunk:
                    self.user_to_family[uid] = f_id

    def generate_users(self):
        print(f"[{self.size_preset}] Writing users.csv...")
        path = os.path.join(self.data_dir, "users.csv")
        with open(path, 'w', newline='') as f:
            writer = csv.writer(f)
            for uid in self.user_ids:
                username = f"{self.fake.user_name()}_{uid}"
                email = f"{username}@{self.fake.free_email_domain()}"
                writer.writerow([uid, username, email, "hash_pw", self._random_date(), '{"theme": "dark"}'])

    def generate_followers(self):
        print(f"[{self.size_preset}] Writing followers.csv...")
        path = os.path.join(self.data_dir, "followers.csv")
        
        # Follower count distribution
        follower_counts = np.random.lognormal(mean=4.2, sigma=0.7, size=self.num_users).astype(int)

        with open(path, 'w', newline='') as f:
            writer = csv.writer(f)
            for uid, count in zip(self.user_ids, follower_counts):
                count = min(count, 1000) # Safety cap
                
                # Distribution logic
                n_fam = min(int(count * 0.6), 22) # Max 22 family members
                n_coll = min(int(count * 0.3), 200)
                n_reg = max(0, count - n_fam - n_coll)

                followed = set()
                
                # Sample Family
                f_id = self.user_to_family[uid]
                fam_pool = [m for m in self.family_members[f_id] if m != uid]
                followed.update(random.sample(fam_pool, min(n_fam, len(fam_pool))))

                # Sample College
                c_id = self.user_to_college[uid]
                coll_pool = [m for m in self.college_members[c_id] if m not in followed and m != uid]
                followed.update(random.sample(coll_pool, min(n_coll, len(coll_pool))))

                # Sample Region (Global Pool)
                r_id = self.user_to_region[uid]
                reg_pool = self.region_members[r_id]
                # To optimize for 'Big' dataset, we don't exclude 'followed' from pool here to save time
                needed = min(n_reg, len(reg_pool))
                for potential in random.sample(reg_pool, needed):
                    if potential != uid:
                        followed.add(potential)

                for f_id in followed:
                    writer.writerow([uid, f_id, self._random_date()])

    def generate_posts_and_tags(self):
        print(f"[{self.size_preset}] Writing posts and tags...")
        posts_path = os.path.join(self.data_dir, "posts.csv")
        tags_path = os.path.join(self.data_dir, "tags.csv")
        post_tags_path = os.path.join(self.data_dir, "post_tags.csv")

        with open(tags_path, 'w', newline='') as f:
            writer = csv.writer(f)
            for i, tag in enumerate(TAGS, 1): writer.writerow([i, tag])

        post_counter = 1
        with open(posts_path, 'w', newline='') as f_p, open(post_tags_path, 'w', newline='') as f_t:
            w_p = csv.writer(f_p)
            w_t = csv.writer(f_t)
            
            # Posts per user (Exponential)
            posts_per_user = np.random.exponential(scale=12, size=self.num_users).astype(int)
            for uid, count in zip(self.user_ids, posts_per_user):
                for _ in range(min(count, 400)):
                    w_p.writerow([post_counter, uid, random.choice(self.lorem_pool), self._random_date()])
                    for t_id in random.sample(range(1, len(TAGS)+1), random.randint(1, 3)):
                        w_t.writerow([post_counter, t_id])
                    self.post_ids.append(post_counter)
                    post_counter += 1

    def generate_engagement(self):
        print(f"[{self.size_preset}] Writing comments and likes...")
        with open(os.path.join(self.data_dir, "comments.csv"), 'w', newline='') as f_c, \
             open(os.path.join(self.data_dir, "post_likes.csv"), 'w', newline='') as f_pl, \
             open(os.path.join(self.data_dir, "comment_likes.csv"), 'w', newline='') as f_cl:
            
            w_c, w_pl, w_cl = csv.writer(f_c), csv.writer(f_pl), csv.writer(f_cl)
            comment_counter = 1

            for pid in self.post_ids:
                pop = np.random.exponential(scale=15)
                n_likes = int(pop * random.uniform(0.3, 2.0))
                n_comments = int(pop * random.uniform(0.1, 1.5))

                if n_likes > 0:
                    for lid in random.sample(self.user_ids, min(n_likes, 500)):
                        w_pl.writerow([pid, lid, self._random_date()])

                for _ in range(n_comments):
                    author = random.choice(self.user_ids)
                    w_c.writerow([comment_counter, pid, author, random.choice(self.lorem_pool), self._random_date()])
                    
                    c_pop = random.randint(0, 8)
                    if c_pop > 0:
                        for clid in random.sample(self.user_ids, min(c_pop, 50)):
                            w_cl.writerow([comment_counter, clid, self._random_date()])
                    comment_counter += 1

    def generate_groups(self):
        print(f"[{self.size_preset}] Writing groups and snowball membership...")
        with open(os.path.join(self.data_dir, "groups.csv"), 'w', newline='') as f_g, \
             open(os.path.join(self.data_dir, "group_members.csv"), 'w', newline='') as f_gm:
            w_g, w_gm = csv.writer(f_g), csv.writer(f_gm)

            num_groups = max(10, self.num_users // 60)
            for gid in range(1, num_groups + 1):
                owner = random.choice(self.user_ids)
                w_g.writerow([gid, f"{random.choice(GROUP_NAMES)} {gid}", random.choice(self.lorem_pool), owner])
                
                # Membership Snowball Logic
                g_size = random.randint(10, 150)
                members = {owner}
                recent_members = [owner]
                
                for _ in range(g_size):
                    if random.random() < 0.8:
                        # Find a "Friend" of a member in the same Region
                        ref_uid = random.choice(recent_members)
                        r_id = self.user_to_region[ref_uid]
                        new_member = random.choice(self.region_members[r_id])
                    else:
                        new_member = random.choice(self.user_ids)
                    
                    if new_member not in members:
                        members.add(new_member)
                        recent_members.append(new_member)
                        w_gm.writerow([gid, new_member, self._random_date()])

    def run(self):
        print(f"\n--- GENERATING DATASET: {self.size_preset} ---")
        self.setup_clusters()
        self.generate_users()
        self.generate_followers()
        self.generate_posts_and_tags()
        self.generate_engagement()
        self.generate_groups()

if __name__ == "__main__":
    for size in ['small', 'medium', 'big']:
        DataGenerator(size).run()
