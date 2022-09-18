#include <chrono>
#include <thread>

#include "epoch.h"

namespace taas
{

    EpochManager* EpochManager::em_ = nullptr;

    EpochManager::EpochManager(int epoch_duration) : epoch_duration_(epoch_duration)
    {
        cur_epoch_ = 0;
        deconstructor_invoked_ = false;
    }

    EpochManager::~EpochManager()
    {
        deconstructor_invoked_ = false;
        delete em_;
    }

    EpochManager::EpochManager(const EpochManager& em)
    {

    }

    EpochManager& EpochManager::operator=(const EpochManager& em)
    {
        return *em_;
    }

    EpochManager* EpochManager::GetInstance()
    {
        if (em_ == nullptr)
        {
            em_ = new EpochManager(10);
        }
        return EpochManager::em_;
    }
    void EpochManager::Run()
    {
        while (!deconstructor_invoked_)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(epoch_duration_));
            cur_epoch_ ++;
        }
    }
    int EpochManager::GetPhysicalEpoch() 
    {
        return cur_epoch_;
    }
} // namespace taas




