/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { OverlayModule } from '@angular/cdk/overlay';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { NzButtonModule } from '../button/nz-button.module';
import { NzAddOnModule } from '../core/addon/addon.module';
import { NzNoAnimationModule } from '../core/no-animation/nz-no-animation.module';
import { NzOverlayModule } from '../core/overlay/nz-overlay.module';
import { NzI18nModule } from '../i18n/nz-i18n.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzPopconfirmComponent } from './nz-popconfirm.component';
import { NzPopconfirmDirective } from './nz-popconfirm.directive';
var NzPopconfirmModule = /** @class */ (function () {
    function NzPopconfirmModule() {
    }
    NzPopconfirmModule.decorators = [
        { type: NgModule, args: [{
                    declarations: [NzPopconfirmComponent, NzPopconfirmDirective],
                    exports: [NzPopconfirmComponent, NzPopconfirmDirective],
                    imports: [
                        CommonModule,
                        NzButtonModule,
                        OverlayModule,
                        NzI18nModule,
                        NzIconModule,
                        NzAddOnModule,
                        NzOverlayModule,
                        NzNoAnimationModule
                    ],
                    entryComponents: [NzPopconfirmComponent]
                },] }
    ];
    return NzPopconfirmModule;
}());
export { NzPopconfirmModule };
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotcG9wY29uZmlybS5tb2R1bGUuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsicG9wY29uZmlybS9uei1wb3Bjb25maXJtLm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3JELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBRXpDLE9BQU8sRUFBRSxjQUFjLEVBQUUsTUFBTSw0QkFBNEIsQ0FBQztBQUM1RCxPQUFPLEVBQUUsYUFBYSxFQUFFLE1BQU0sNEJBQTRCLENBQUM7QUFDM0QsT0FBTyxFQUFFLG1CQUFtQixFQUFFLE1BQU0sNkNBQTZDLENBQUM7QUFDbEYsT0FBTyxFQUFFLGVBQWUsRUFBRSxNQUFNLG1DQUFtQyxDQUFDO0FBQ3BFLE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUN0RCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFFdEQsT0FBTyxFQUFFLHFCQUFxQixFQUFFLE1BQU0sMkJBQTJCLENBQUM7QUFDbEUsT0FBTyxFQUFFLHFCQUFxQixFQUFFLE1BQU0sMkJBQTJCLENBQUM7QUFFbEU7SUFBQTtJQWVpQyxDQUFDOztnQkFmakMsUUFBUSxTQUFDO29CQUNSLFlBQVksRUFBRSxDQUFDLHFCQUFxQixFQUFFLHFCQUFxQixDQUFDO29CQUM1RCxPQUFPLEVBQUUsQ0FBQyxxQkFBcUIsRUFBRSxxQkFBcUIsQ0FBQztvQkFDdkQsT0FBTyxFQUFFO3dCQUNQLFlBQVk7d0JBQ1osY0FBYzt3QkFDZCxhQUFhO3dCQUNiLFlBQVk7d0JBQ1osWUFBWTt3QkFDWixhQUFhO3dCQUNiLGVBQWU7d0JBQ2YsbUJBQW1CO3FCQUNwQjtvQkFDRCxlQUFlLEVBQUUsQ0FBQyxxQkFBcUIsQ0FBQztpQkFDekM7O0lBQ2dDLHlCQUFDO0NBQUEsQUFmbEMsSUFla0M7U0FBckIsa0JBQWtCIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgT3ZlcmxheU1vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2Nkay9vdmVybGF5JztcbmltcG9ydCB7IENvbW1vbk1vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2NvbW1vbic7XG5pbXBvcnQgeyBOZ01vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuXG5pbXBvcnQgeyBOekJ1dHRvbk1vZHVsZSB9IGZyb20gJy4uL2J1dHRvbi9uei1idXR0b24ubW9kdWxlJztcbmltcG9ydCB7IE56QWRkT25Nb2R1bGUgfSBmcm9tICcuLi9jb3JlL2FkZG9uL2FkZG9uLm1vZHVsZSc7XG5pbXBvcnQgeyBOek5vQW5pbWF0aW9uTW9kdWxlIH0gZnJvbSAnLi4vY29yZS9uby1hbmltYXRpb24vbnotbm8tYW5pbWF0aW9uLm1vZHVsZSc7XG5pbXBvcnQgeyBOek92ZXJsYXlNb2R1bGUgfSBmcm9tICcuLi9jb3JlL292ZXJsYXkvbnotb3ZlcmxheS5tb2R1bGUnO1xuaW1wb3J0IHsgTnpJMThuTW9kdWxlIH0gZnJvbSAnLi4vaTE4bi9uei1pMThuLm1vZHVsZSc7XG5pbXBvcnQgeyBOekljb25Nb2R1bGUgfSBmcm9tICcuLi9pY29uL256LWljb24ubW9kdWxlJztcblxuaW1wb3J0IHsgTnpQb3Bjb25maXJtQ29tcG9uZW50IH0gZnJvbSAnLi9uei1wb3Bjb25maXJtLmNvbXBvbmVudCc7XG5pbXBvcnQgeyBOelBvcGNvbmZpcm1EaXJlY3RpdmUgfSBmcm9tICcuL256LXBvcGNvbmZpcm0uZGlyZWN0aXZlJztcblxuQE5nTW9kdWxlKHtcbiAgZGVjbGFyYXRpb25zOiBbTnpQb3Bjb25maXJtQ29tcG9uZW50LCBOelBvcGNvbmZpcm1EaXJlY3RpdmVdLFxuICBleHBvcnRzOiBbTnpQb3Bjb25maXJtQ29tcG9uZW50LCBOelBvcGNvbmZpcm1EaXJlY3RpdmVdLFxuICBpbXBvcnRzOiBbXG4gICAgQ29tbW9uTW9kdWxlLFxuICAgIE56QnV0dG9uTW9kdWxlLFxuICAgIE92ZXJsYXlNb2R1bGUsXG4gICAgTnpJMThuTW9kdWxlLFxuICAgIE56SWNvbk1vZHVsZSxcbiAgICBOekFkZE9uTW9kdWxlLFxuICAgIE56T3ZlcmxheU1vZHVsZSxcbiAgICBOek5vQW5pbWF0aW9uTW9kdWxlXG4gIF0sXG4gIGVudHJ5Q29tcG9uZW50czogW056UG9wY29uZmlybUNvbXBvbmVudF1cbn0pXG5leHBvcnQgY2xhc3MgTnpQb3Bjb25maXJtTW9kdWxlIHt9XG4iXX0=