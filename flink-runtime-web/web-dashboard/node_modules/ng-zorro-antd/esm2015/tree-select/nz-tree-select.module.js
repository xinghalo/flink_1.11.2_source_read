/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { OverlayModule } from '@angular/cdk/overlay';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { NzNoAnimationModule } from '../core/no-animation/nz-no-animation.module';
import { NzOverlayModule } from '../core/overlay/nz-overlay.module';
import { NzEmptyModule } from '../empty/nz-empty.module';
import { NzIconModule } from '../icon/nz-icon.module';
import { NzTreeModule } from '../tree/nz-tree.module';
import { NzTreeSelectComponent } from './nz-tree-select.component';
export class NzTreeSelectModule {
}
NzTreeSelectModule.decorators = [
    { type: NgModule, args: [{
                imports: [
                    CommonModule,
                    OverlayModule,
                    FormsModule,
                    NzTreeModule,
                    NzIconModule,
                    NzEmptyModule,
                    NzOverlayModule,
                    NzNoAnimationModule
                ],
                declarations: [NzTreeSelectComponent],
                exports: [NzTreeSelectComponent]
            },] }
];
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotdHJlZS1zZWxlY3QubW9kdWxlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbInRyZWUtc2VsZWN0L256LXRyZWUtc2VsZWN0Lm1vZHVsZS50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7O0FBQUEsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLHNCQUFzQixDQUFDO0FBQ3JELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSxpQkFBaUIsQ0FBQztBQUMvQyxPQUFPLEVBQUUsUUFBUSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQ3pDLE9BQU8sRUFBRSxXQUFXLEVBQUUsTUFBTSxnQkFBZ0IsQ0FBQztBQUM3QyxPQUFPLEVBQUUsbUJBQW1CLEVBQUUsTUFBTSw2Q0FBNkMsQ0FBQztBQUNsRixPQUFPLEVBQUUsZUFBZSxFQUFFLE1BQU0sbUNBQW1DLENBQUM7QUFDcEUsT0FBTyxFQUFFLGFBQWEsRUFBRSxNQUFNLDBCQUEwQixDQUFDO0FBQ3pELE9BQU8sRUFBRSxZQUFZLEVBQUUsTUFBTSx3QkFBd0IsQ0FBQztBQUN0RCxPQUFPLEVBQUUsWUFBWSxFQUFFLE1BQU0sd0JBQXdCLENBQUM7QUFDdEQsT0FBTyxFQUFFLHFCQUFxQixFQUFFLE1BQU0sNEJBQTRCLENBQUM7QUFnQm5FLE1BQU0sT0FBTyxrQkFBa0I7OztZQWQ5QixRQUFRLFNBQUM7Z0JBQ1IsT0FBTyxFQUFFO29CQUNQLFlBQVk7b0JBQ1osYUFBYTtvQkFDYixXQUFXO29CQUNYLFlBQVk7b0JBQ1osWUFBWTtvQkFDWixhQUFhO29CQUNiLGVBQWU7b0JBQ2YsbUJBQW1CO2lCQUNwQjtnQkFDRCxZQUFZLEVBQUUsQ0FBQyxxQkFBcUIsQ0FBQztnQkFDckMsT0FBTyxFQUFFLENBQUMscUJBQXFCLENBQUM7YUFDakMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBPdmVybGF5TW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY2RrL292ZXJsYXknO1xuaW1wb3J0IHsgQ29tbW9uTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29tbW9uJztcbmltcG9ydCB7IE5nTW9kdWxlIH0gZnJvbSAnQGFuZ3VsYXIvY29yZSc7XG5pbXBvcnQgeyBGb3Jtc01vZHVsZSB9IGZyb20gJ0Bhbmd1bGFyL2Zvcm1zJztcbmltcG9ydCB7IE56Tm9BbmltYXRpb25Nb2R1bGUgfSBmcm9tICcuLi9jb3JlL25vLWFuaW1hdGlvbi9uei1uby1hbmltYXRpb24ubW9kdWxlJztcbmltcG9ydCB7IE56T3ZlcmxheU1vZHVsZSB9IGZyb20gJy4uL2NvcmUvb3ZlcmxheS9uei1vdmVybGF5Lm1vZHVsZSc7XG5pbXBvcnQgeyBOekVtcHR5TW9kdWxlIH0gZnJvbSAnLi4vZW1wdHkvbnotZW1wdHkubW9kdWxlJztcbmltcG9ydCB7IE56SWNvbk1vZHVsZSB9IGZyb20gJy4uL2ljb24vbnotaWNvbi5tb2R1bGUnO1xuaW1wb3J0IHsgTnpUcmVlTW9kdWxlIH0gZnJvbSAnLi4vdHJlZS9uei10cmVlLm1vZHVsZSc7XG5pbXBvcnQgeyBOelRyZWVTZWxlY3RDb21wb25lbnQgfSBmcm9tICcuL256LXRyZWUtc2VsZWN0LmNvbXBvbmVudCc7XG5cbkBOZ01vZHVsZSh7XG4gIGltcG9ydHM6IFtcbiAgICBDb21tb25Nb2R1bGUsXG4gICAgT3ZlcmxheU1vZHVsZSxcbiAgICBGb3Jtc01vZHVsZSxcbiAgICBOelRyZWVNb2R1bGUsXG4gICAgTnpJY29uTW9kdWxlLFxuICAgIE56RW1wdHlNb2R1bGUsXG4gICAgTnpPdmVybGF5TW9kdWxlLFxuICAgIE56Tm9BbmltYXRpb25Nb2R1bGVcbiAgXSxcbiAgZGVjbGFyYXRpb25zOiBbTnpUcmVlU2VsZWN0Q29tcG9uZW50XSxcbiAgZXhwb3J0czogW056VHJlZVNlbGVjdENvbXBvbmVudF1cbn0pXG5leHBvcnQgY2xhc3MgTnpUcmVlU2VsZWN0TW9kdWxlIHt9XG4iXX0=