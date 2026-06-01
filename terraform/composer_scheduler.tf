#=========================================
# COMPOSER AUTO PAUSE / RESUME
#
# Schedule:
#   23:00 UTC  → resume  (1 hour before @daily midnight run)
#   03:00 UTC  → pause   (3 hours after midnight, pipeline should be done)
#
# Architecture:
#   Cloud Scheduler → Cloud Run Job (google/cloud-sdk) → gcloud composer update
#=========================================

# ── Service Account ────────────────────────────────────────────────────────────
resource "google_service_account" "composer_scheduler" {
  count        = var.environment == "gcp" ? 1 : 0
  account_id   = "composer-scheduler"
  display_name = "Composer Pause/Resume Scheduler"
  project      = var.project_id
}

# ── IAM ───────────────────────────────────────────────────────────────────────

# Allows the Cloud Run Job container to call the Composer API
resource "google_project_iam_member" "composer_scheduler_composer_admin" {
  count   = var.environment == "gcp" ? 1 : 0
  project = var.project_id
  role    = "roles/composer.admin"
  member  = "serviceAccount:${google_service_account.composer_scheduler[0].email}"
}

# Allows Cloud Scheduler to trigger the Cloud Run Jobs
resource "google_project_iam_member" "composer_scheduler_run_invoker" {
  count   = var.environment == "gcp" ? 1 : 0
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.composer_scheduler[0].email}"
}

# ── Cloud Run Jobs ─────────────────────────────────────────────────────────────

resource "google_cloud_run_v2_job" "composer_pause" {
  count    = var.environment == "gcp" ? 1 : 0
  name     = "composer-pause"
  location = var.region

  template {
    template {
      service_account = google_service_account.composer_scheduler[0].email
      max_retries     = 1

      containers {
        image   = "google/cloud-sdk:slim"
        command = ["gcloud"]
        args = [
          "composer", "environments", "update",
          "${var.project_id}-composer",
          "--location=${var.region}",
          "--project=${var.project_id}",
          "--pause",
          "--async",
          "--quiet",
        ]
      }
    }
  }

  depends_on = [google_composer_environment.composer]
}

resource "google_cloud_run_v2_job" "composer_resume" {
  count    = var.environment == "gcp" ? 1 : 0
  name     = "composer-resume"
  location = var.region

  template {
    template {
      service_account = google_service_account.composer_scheduler[0].email
      max_retries     = 1

      containers {
        image   = "google/cloud-sdk:slim"
        command = ["gcloud"]
        args = [
          "composer", "environments", "update",
          "${var.project_id}-composer",
          "--location=${var.region}",
          "--project=${var.project_id}",
          "--resume",
          "--async",
          "--quiet",
        ]
      }
    }
  }

  depends_on = [google_composer_environment.composer]
}

# ── Cloud Scheduler ───────────────────────────────────────────────────────────

resource "google_cloud_scheduler_job" "composer_pause" {
  count     = var.environment == "gcp" ? 1 : 0
  name      = "composer-daily-pause"
  region    = var.region
  schedule  = "0 3 * * *"
  time_zone = "UTC"

  http_target {
    http_method = "POST"
    uri = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.composer_pause[0].name}:run"

    oauth_token {
      service_account_email = google_service_account.composer_scheduler[0].email
    }
  }
}

resource "google_cloud_scheduler_job" "composer_resume" {
  count     = var.environment == "gcp" ? 1 : 0
  name      = "composer-daily-resume"
  region    = var.region
  schedule  = "0 23 * * *"
  time_zone = "UTC"

  http_target {
    http_method = "POST"
    uri = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.composer_resume[0].name}:run"

    oauth_token {
      service_account_email = google_service_account.composer_scheduler[0].email
    }
  }
}
